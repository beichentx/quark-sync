#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
夸克网盘 -> fnOS 单向下载同步引擎（带网页监控）
功能：
  - 每 INTERVAL 秒巡检一次，把夸克目录下新增/未下完的文件同步到fnOS本地
  - 文件级并发下载（CONCURRENT 个同时跑）
  - 每个文件独立显示：进度条、已下载、总大小、速度、预计剩余时间
  - 每个文件可单独 暂停 / 启动 / 重启
  - 全局 暂停 / 恢复 / 重启
  - 断点续传（rsync --partial-dir）
  - 可选：拉完删网盘源文件 / 删源目录 / 删空目录（均不影响本地）
  - 停滞自愈：单任务超过 STALL_SECONDS 无进度自动重启（防夸克限流导致挂死）
  - 优先级启动(v1.2.12)：网页点"启动/重启"的任务插队优先下载；
    并发槽满时自动暂停完成度最低的 running 任务让出槽位
  - 僵尸任务清理(v1.2.12)：网盘侧删掉源文件后，任务连续 2 轮扫描不到源文件
    即自动移除记录；网页端也可手动"移除"单条 / "清除失败"批量清理
  - 优雅停止(v1.2.13)：容器停止/重启/网页"重启同步"时，先通知所有 rsync 优雅退出、
    把断点保存进 .rsync-partial/ 再退出——否则 rsync 被 SIGKILL 硬杀，
    半成品变成孤儿临时文件（.<文件名>.XXXXXX），下次只能整文件重下
  - 断点抢救(v1.2.13)：启动任务前发现历史硬中断遗留的孤儿临时文件，
    自动把最大的一个挪进 .rsync-partial/ 当断点续传，更小的重复文件清理；
    每轮巡检还会清扫已确定无用的孤儿临时文件（文件已完成/源已删除的）
  - 文件夹级启动让位(v1.2.14)：点目录"启动"时 pending 任务也整体插队，
    并发槽被其他目录占满时自动暂停完成度最低的 running 任务让位——
    保证点完立刻能看到该目录的任务开跑（旧版只处理 paused/error 且不让位，
    槽位被占满时点启动毫无效果，是"点击上级目录启动无效"的根因）
  - Web 可感知反馈(v1.2.14)：目录展开过渡动画、操作结果 toast 提示、
    按钮成功闪烁（借鉴 Toastify 的 toast 模式），点没点中都一眼可见
  - 日志心跳：进度每跨 10% 打印一次，当前下载任务每 HEARTBEAT_SECONDS 汇总一次
  - 删源保护(v1.2.16)：删源粒度从"单个文件"提到"番剧/剧集目录"——
    目录还在冒新文件（连载中）→ 保留网盘源；静默超过 DELETE_SRC_QUIET_DAYS 天
    （视为已完结）→ 放行删除回收网盘空间。另支持外部保留名单 KEEP_SRC_FILE
    （由元数据完结检测脚本 / 人工维护）精确锁定任何不许删的路径。
    判定单元是目录：因为"这一集下完了"≠"这部番完结了"。
  - 建任务预检(v1.6)：扫描建任务时发现目标文件已存在且尺寸达标 → 建任务即标
    done、不入队（附"预检:目标已满"备注），源文件照常走删源闸门；引擎重启后
    已下满的文件不再重新排队（旧版仅靠 rsync --size-only 空跑兜底，队列里全
    是假 pending）。v1.6.1 加改名兜底：dst 同目录存在同字节数文件（TMDB 改名
    版/订阅退场后 spec 消失回落 plain 名的场景）同样视为已下载，防止真下载
    一遍 plain 名副本。
  - 唯一集数豁免(v1.6.1)：广告大小过滤（中位数×ratio）不再误杀"该集数在全目录
    仅此一份"的小体积正片（电锯人 E01/E10 小体积 mp4 被误杀只剩 10 集的事故）；
    豁免仍要求 ≥1MB 以挡纯垃圾文件。

Web 监控：http://<NAS_IP>:<PORT>（端口可用环境变量 PORT 修改，默认 49999）
API：
  GET  /          监控页面
  GET  /status    JSON 状态
  POST /control?action=pause|resume|restart
  POST /control?action=pause_file&file=FOLDER/相对路径
  POST /control?action=start_file&file=FOLDER/相对路径
  POST /control?action=restart_file&file=FOLDER/相对路径
  POST /control?action=remove_file&file=FOLDER/相对路径   移除任务记录（不影响本地文件）
  POST /control?action=clear_errors                       移除全部"失败"任务
"""

import json
import os
import re
import signal
import subprocess
import threading
import time
import urllib.parse
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

# -----------------------------------------------------------------------------
# 配置（来自环境变量）
# -----------------------------------------------------------------------------
SRC_BASE = os.environ.get("SRC_BASE", "/mnt/quark")
DST_BASE = os.environ.get("DST_BASE", "/sync/quark")
FOLDERS = os.environ.get("FOLDERS", "").split()
INTERVAL = int(os.environ.get("INTERVAL", "600"))
CONCURRENT = int(os.environ.get("CONCURRENT", "5"))
DELETE = os.environ.get("DELETE", "false").lower() == "true"
DELETE_SRC = os.environ.get("DELETE_SRC", "false")
DELETE_SRC_DIR = os.environ.get("DELETE_SRC_DIR", "false")
DELETE_EMPTY_DIR = os.environ.get("DELETE_EMPTY_DIR", "true")
# 空目录清理的"存活时长"保护(秒)：只清理存在时间超过该值的空目录，
# 避免误删网盘侧刚新建、还没来得及上传文件的文件夹（P1-2）。0=关闭保护(旧行为)
EMPTY_DIR_MIN_AGE = int(os.environ.get("EMPTY_DIR_MIN_AGE", "3600"))

# ---- 按番剧/剧集粒度的删源策略（v1.2.16）---------------------------------
# 背景：追番场景下"下完即删源"会把还在更新的番的源也删掉——下一集转存进来时
# 分享链接里的旧集会被重复转存（本地虽会跳过，但白占网盘空间和转存配额），
# 且本地若不慎损坏就再也拿不回源。期望行为是：
#   连载中（还在更新）→ 保留网盘源；  已完结 → 下完即删，回收网盘空间。
# 判定分两层，任一判定"未完结"即保留源：
#   ① 外部保留名单 KEEP_SRC_FILE（精确，由完结检测脚本 / 人工维护）
#   ② 静默期 DELETE_SRC_QUIET_DAYS（兜底，纯本地观测，不依赖任何 API）
#
# 静默期删源：番剧目录（FOLDER/一级子目录）超过 N 天没有新文件流入，视为
# "已完结/已停更"，放行删除网盘源；仍在冒新文件的目录则保留源。0=关闭（保持
# 旧的"下完即删"行为，向后兼容）。
DELETE_SRC_QUIET_DAYS = int(os.environ.get("DELETE_SRC_QUIET_DAYS", "0"))
# 只对这些父目录启用静默期保护（逗号分隔）；留空 = 所有 FOLDERS 都启用。
# 典型 DONGMAN,TV —— 电影/音乐/游戏没有"连载"概念，保持下完即删。
DELETE_SRC_QUIET_FOLDERS = [x.strip() for x in os.environ.get("DELETE_SRC_QUIET_FOLDERS", "").split(",") if x.strip()]
# 外部保留名单文件（JSON）：{"keep": ["DONGMAN/葬送", ...], "reason": {"DONGMAN/葬送": "..."}}
# keep 里是"路径前缀"，命中的源一律不删。每轮巡检热加载（文件 mtime 变了才重读），
# 改名单不用重启容器。⚠️ 文件存在但解析失败 → 暂停全部删源并告警：
# 删源不可逆，"名单读坏了"绝不能被解释成"名单是空的"。
KEEP_SRC_FILE = os.environ.get("KEEP_SRC_FILE", "")
# 每个目录"最后活跃时刻"的持久化记录（引擎自观测：目录内文件集合变了就刷新）。
# 这是静默期判定的第二路信号——不依赖 FUSE/网盘返回的 mtime 是否可靠。
# 与 STATE_DIR 同目录，容器重建后会丢（重建后重新建基线，见 _load_activity 说明）。
ACTIVITY_FILE = os.environ.get("ACTIVITY_FILE", "")
MAX_RETRY = int(os.environ.get("MAX_RETRY", "2"))
STALL_SECONDS = int(os.environ.get("STALL_SECONDS", "180"))     # 单任务超过N秒无任何字节推进=停滞，自动重启(自愈)
STALL_RETRY = int(os.environ.get("STALL_RETRY", "3"))           # 连续停滞N次后本轮放弃，等下一轮巡检
HEARTBEAT_SECONDS = int(os.environ.get("HEARTBEAT_SECONDS", "30"))  # 日志里每N秒打印一次当前下载汇总
# 低速让位：单任务持续龟速（真实下载速率低于 LOW_SPEED_BPS 字节/秒）超过 LOW_SPEED_SECONDS 秒，
# 说明被挂载/网盘限速（AList WebDAV 限速的典型症状），占着并发槽却跑不动，队列里其他任务永远排不上。
# 此时把它置 error 让位，等下一轮巡检重试（期间其他任务先跑）。0 = 关闭低速让位。
LOW_SPEED_BPS = int(os.environ.get("LOW_SPEED_BPS", str(300 * 1024)))   # 默认 300KB/s 以下算龟速
LOW_SPEED_SECONDS = int(os.environ.get("LOW_SPEED_SECONDS", "300"))     # 默认持续 5 分钟让位
# 连续停滞达到 N 次时，除杀 rsync 重启外，再给挂载进程发 SIGHUP 重置 FUSE 连接。
# 根因是挂载点级限速/卡死：只杀 rsync 重启还会读同一个卡死的 FUSE 挂载点（帕金森循环）。
# 重置挂载连接等价于手动 docker restart 里"重启 rclone 挂载"那一步——用户实测卡死只有
# 重启容器才解冻，正是因为这步。默认 2：首次停滞只重启 rsync（轻量），二次仍卡才重置挂载；
# 设 1 则每次停滞都重置（更激进、更接近手动重启体感）。需 compose 配 pid: host 才能看到宿主 rclone。
STALL_MOUNT_RESET = int(os.environ.get("STALL_MOUNT_RESET", "2"))
# FUSE 缓存刷新的目标进程名（逗号分隔，按序匹配）。fnOS的夸克/网盘远程挂载实际由
# `rclone rcd` 进程承载（/proc/PID/comm 是 `rclone`，`rcd` 是其子命令；`/etc/mountmgr/`
# 是fnOS内部的配置目录，不是进程名）。保留 `mountmgr` 兼容极老的fnOS版本。
PROC_NAMES = [n.strip() for n in os.environ.get("PROC_NAMES", "mountmgr,rclone,rcd").split(",") if n.strip()]
PUSH_URL = os.environ.get("PUSH_URL", "")

STATE_DIR = os.environ.get("STATE_DIR", "/tmp/quark_sync")
STATUS_FILE = os.path.join(STATE_DIR, "status.json")
CONTROL_FILE = os.path.join(STATE_DIR, "control")
PORT = int(os.environ.get("PORT", "49999"))   # Web 监控端口

# 持久化可选目录：compose 把宿主目录挂到 /state 时，把"跨重启必须存活"的东西放这里。
# STATE_DIR 在容器内是 /tmp，容器一重建就没了——活跃度记录丢一次意味着静默期
# 从头重新计时（14 天），所以它必须落在挂载卷上。没挂 /state 时退回 STATE_DIR
# （功能可用，但容器重建后重新建基线）。
PERSIST_DIR = "/state" if os.path.isdir("/state") else STATE_DIR
if not ACTIVITY_FILE:
    ACTIVITY_FILE = os.path.join(PERSIST_DIR, "activity.json")
if not KEEP_SRC_FILE:
    _default_keep = os.path.join(PERSIST_DIR, "keep_src.json")
    KEEP_SRC_FILE = _default_keep if os.path.exists(_default_keep) else ""

# ---- v1.4 下载侧逐文件分流（剧场版/OVA/广告过滤/TMDB命名）----
# 条目元数据由 quark-pipeline 编排写入 /state/filter_shows.json，引擎每轮巡检热加载
# （mtime 变了才重读）。shows 键 = 网盘目录 "FOLDER/一级子目录"；不在名单里的目录
# 走传统行为（全量下载），完全向后兼容。
SHOW_SPEC_FILE = os.environ.get("SHOW_SPEC_FILE", "")
if not SHOW_SPEC_FILE:
    _default_spec = os.path.join(PERSIST_DIR, "filter_shows.json")
    SHOW_SPEC_FILE = _default_spec if os.path.exists(_default_spec) else ""
MOVIE_DIRNAME = os.environ.get("MOVIE_DIRNAME", "MOVIE")     # DST_BASE 下的电影库目录名
AD_SIZE_RATIO = float(os.environ.get("AD_SIZE_RATIO", "0.2"))   # 视频大小 < 同目录中位数×该比例 → 广告
AD_ABS_FLOOR_MB = int(os.environ.get("AD_ABS_FLOOR_MB", "30"))  # 无中位数参照时的绝对下限(MB)

# -----------------------------------------------------------------------------
# 工具函数
# -----------------------------------------------------------------------------

def log(msg):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}", flush=True)

def notify(msg):
    if not PUSH_URL:
        return
    try:
        subprocess.run(
            ["curl", "-s", "-m", "10", "-X", "POST", PUSH_URL,
             "--data-urlencode", f"msg={msg}"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=12
        )
    except Exception:
        pass

def parse_size(s):
    """把 rsync 的 1.23K / 4.56M / 7.89G 转成字节数"""
    if not s:
        return 0
    s = s.strip().replace(",", "")
    units = {"B": 1, "K": 1024, "M": 1024**2, "G": 1024**3, "T": 1024**4}
    m = re.match(r"^(\d+(?:\.\d+)?)\s*([BKMGTP]?)\s*$", s, re.I)
    if not m:
        return 0
    val, unit = float(m.group(1)), (m.group(2) or "B").upper()
    return int(val * units.get(unit, 1))

def fmt_size(n):
    for u in ["B", "K", "M", "G", "T"]:
        if n < 1024 or u == "T":
            return f"{n:.2f}{u}" if u != "B" else f"{int(n)}B"
        n /= 1024

def fmt_speed(bps):
    """把 B/s 字节速率格式化为人类可读字符串"""
    n = float(bps)
    for u in ["B/s", "KB/s", "MB/s", "GB/s", "TB/s"]:
        if n < 1024 or u == "TB/s":
            return f"{n:.2f}{u}" if u != "B/s" else f"{int(n)}B/s"
        n /= 1024

def fmt_eta(s):
    if not s or s == "-" or s == "?":
        return "计算中"
    try:
        parts = s.split(":")
        if len(parts) == 2:
            return f"{parts[0]}分{parts[1]}秒"
        if len(parts) == 3:
            return f"{parts[0]}时{parts[1]}分"
    except Exception:
        pass
    return s

# -----------------------------------------------------------------------------
# v1.4 逐文件分流：分类 + TMDB 可识别命名（纯函数，可独立测试）
# -----------------------------------------------------------------------------
# 用户决策（2026-09-18）：
#   ① 剧场版从下载就搬进电影库（DST_BASE/MOVIE/<片名 剧场版>/），每条目可配 route/skip/keep；
#   ② 广告小视频 = 扩展名白名单外的视频 + 大小 < 同目录中位数×0.2 + 关键词黑名单；
#   ③ TMDB/刮削器无法识别的东西（NCOP/NCED/PV/特典/菜单/音频/垃圾文件）一律不下载；
#   ④ OVA/OAD 落单独 OVA/ 子目录；
#   ⑤ 正片按 "<标题> SxxEyy.ext" 重命名（集数从真实文件名解析，季数从子目录解析）。

VIDEO_EXTS = {".mkv", ".mp4", ".ts", ".m2ts", ".avi", ".rmvb", ".flv", ".webm", ".wmv", ".mov", ".mpg", ".mpeg"}
SUB_EXTS = {".ass", ".srt", ".ssa", ".sup", ".sub", ".vtt"}
META_EXTS = {".nfo"}          # 只保留 nfo；图片(jpg/png等)一律不下载（多为广告图，海报由fnOS影视自刮）
AUDIO_EXTS = {".mp3", ".flac", ".m4a", ".aac", ".wav", ".ogg", ".ape", ".tta", ".tak"}
TRASH_EXTS = {".exe", ".lnk", ".url", ".html", ".htm", ".txt", ".doc", ".docx", ".pdf",
              ".zip", ".rar", ".7z", ".bat", ".apk", ".torrent", ".db", ".ini", ".sfv"}

AD_KW_RE = re.compile(
    r"公众号|扫码|关注|订阅|推广|宣传|广告|防走丢|更多资源|获取更多|必看|大合集|资源合集"
    r"|\bsample\b|\bpreview\b|\btrailer\b|\bpromo\b", re.I)
# 附加内容（刮削器无法识别为正片）：NCOP/NCED/菜单/特典/PV/CM/预告/Credits
EXTRA_RE = re.compile(
    r"ncop|nced|\bmenu\b|特典|預告|预告|メニュー|\bcredits?\b|billboard"
    r"|(?<![A-Za-z0-9])(PV|CM|NC)(?![A-Za-z0-9])"
    r"|(?<![A-Za-z0-9])(OP|ED)\d{0,2}(?![A-Za-z0-9])", re.I)
MOVIE_RE = re.compile(r"剧场版|劇場版|映画|gekijou|gekijyouban|(?<![A-Za-z])movie(?![A-Za-z])", re.I)
OVA_RE = re.compile(r"(?<![A-Za-z])(ova|oad)(?![A-Za-z])|sp特典|特別編|特别篇", re.I)

_CN_NUM = {"一": 1, "二": 2, "三": 3, "四": 4, "五": 5, "六": 6, "七": 7, "八": 8, "九": 9, "十": 10}


def parse_episode(name):
    """从文件名解析集数。返回 (ep:int|None, ver:int)。识别不了返回 (None, 1)。

    覆盖：S01E05 / 01.mkv（纯数字名）/ 第05话(集/回) / E05、EP05 / [05] / - 05 结尾或分隔。
    注意顺序：SxxEyy 优先于裸数字（避免把 "1080p" 这类误当集数——它没有 E 前缀）。
    """
    stem = os.path.splitext(os.path.basename(name))[0]
    # 字幕常见命名 "05.chs.ass"：splitext 后 stem 还挂着语言尾巴，剥掉再解析
    stem = re.sub(r"\.(chs|cht|sc|tc|gb|big5|zho|zh|jpn|jp|jap|eng|kor|kr)$", "", stem, flags=re.I)
    ver = 1
    m = re.search(r"(?<![0-9A-Za-z])[Ss]\d{1,2}\s?[Ee][Pp]?\s?(\d{1,3})(?:[Vv](\d))?(?![0-9])", stem)
    if m:
        return int(m.group(1)), int(m.group(2) or 1)
    if re.fullmatch(r"(\d{1,3})(?:[Vv](\d))?", stem.strip()):
        m = re.fullmatch(r"(\d{1,3})(?:[Vv](\d))?", stem.strip())
        return int(m.group(1)), int(m.group(2) or 1)
    m = re.search(r"第\s*(\d{1,3})\s*[话話集回]", stem)
    if m:
        return int(m.group(1)), 1
    m = re.search(r"(?<![0-9A-Za-z])[Ee][Pp]?\.?\s?(\d{1,3})(?:[Vv](\d))?(?![0-9pP])", stem)
    if m:
        return int(m.group(1)), int(m.group(2) or 1)
    m = re.search(r"[\[\（(]\s?(\d{1,3})(?:[Vv](\d))?\s?[\]\）)]", stem)
    if m:
        return int(m.group(1)), int(m.group(2) or 1)
    m = re.search(r"[-–]\s?(\d{1,3})(?:[Vv](\d))?(?=\s*[\[\(（]|\s*$)", stem)
    if m:
        return int(m.group(1)), int(m.group(2) or 1)
    return None, ver


def season_from_path(relpath):
    """从相对路径（目录部分）解析季数。中文数字/Season N/SN 均支持，默认 1。"""
    season = 1
    for seg in (relpath or "").replace(os.sep, "/").split("/"):
        m = re.search(r"第\s*(\d{1,2}|[一二三四五六七八九十]+)\s*季", seg)
        if m:
            v = m.group(1)
            season = int(v) if v.isdigit() else _CN_NUM.get(v, season)
            continue
        m = re.search(r"[Ss]eason\s*(\d{1,2})", seg)
        if m:
            season = int(m.group(1))
            continue
        m = re.search(r"(?<![A-Za-z0-9])[Ss](\d{1,2})(?![0-9])", seg)
        if m:
            season = int(m.group(1))
    return season if 0 < season < 100 else 1


def classify_file(name, size, dir_median=0, cfg=None, unique_ep=False):
    """单文件分类。返回 tag ∈ main/raw/movie/ova/sub/meta/skip。

    main=正片（可解析集数，重命名）；raw=大视频但解析不出集数（保留原名下载）；
    movie/ova=剧场版/OVA；sub/meta=字幕/刮削元数据（下载）；skip=不要。
    size 判定只对视频类生效：目录中位数为 0 时退回绝对下限。
    unique_ep=True 表示该集数在全目录仅此一个文件（由调用方扫描时统计传入），
    小体积豁免见下方判定。
    """
    cfg = cfg or {}
    ratio = float(cfg.get("ad_size_ratio", AD_SIZE_RATIO))
    floor = int(cfg.get("ad_abs_floor_mb", AD_ABS_FLOOR_MB)) * 1024 * 1024
    e = os.path.splitext(name)[1].lower()
    base = os.path.basename(name)
    if e in TRASH_EXTS:
        return "skip"
    if e in SUB_EXTS:
        return "sub"
    if e in META_EXTS:
        return "meta"
    if e in AUDIO_EXTS:
        return "skip"
    if e not in VIDEO_EXTS:
        return "skip"
    if MOVIE_RE.search(base):
        return "movie"
    if OVA_RE.search(base):
        return "ova"
    if EXTRA_RE.search(base) or AD_KW_RE.search(base):
        return "skip"
    threshold = int(ratio * dir_median) if dir_median > 0 else floor
    # 唯一集数豁免（v1.6.1）：该集数在全目录只有这一个文件时，小体积不再当广告——
    # 真广告是"正片旁的重复小副本"，不会是唯一一集（电锯人 E01/E10 小体积 mp4
    # 被中位数×0.2 误杀 → 只下 10 集的事故）。豁免仍要求 ≥1MB 挡纯垃圾。
    if size < threshold and not (unique_ep and size >= 1_000_000):
        return "skip"
    ep, _ver = parse_episode(base)
    return "main" if ep is not None else "raw"


def _has_cjk(s):
    return any("\u4e00" <= ch <= "\u9fff" for ch in s)


def _sub_lang(base):
    low = base.lower()
    if "cht" in low or "繁" in base or "zh-tw" in low or "big5" in low:
        return "cht"
    if "jpn" in low or "jp" in low or "日" in base or "jap" in low:
        return "jpn"
    if "kor" in low or "kr" in low or "韩" in base:
        return "kor"
    return "chs"


def build_task_dst(folder, rel, size, dir_median, show, dst_base, movie_dirname=MOVIE_DIRNAME,
                   ep_counts=None):
    """按分类计算任务最终落盘路径。返回 (action, dst)：
    action ∈ skip（不建任务）/ plain（原名下载，dst=None）/ dst（下载到指定路径）。
    show 为 None 时走 plain（未在分流名单里的目录 = 传统行为）。
    ep_counts：调用方扫描时统计的 {集数: 该目录内文件数}，供唯一集数豁免；缺省不豁免。
    """
    if show is None:
        return "plain", None
    # 唯一集数豁免（v1.6.1）：该集数目录内唯一 → 不按小体积广告过滤
    unique = False
    if ep_counts is not None:
        _ep, _ = parse_episode(os.path.basename(rel))
        unique = _ep is not None and ep_counts.get(_ep, 0) == 1
    tag = classify_file(rel, size, dir_median, show.get("_cfg"), unique_ep=unique)
    if tag == "skip":
        return "skip", None
    if (show.get("type") or "").lower() == "movie":
        # 电影条目：只做垃圾过滤（广告图/垃圾文件/异常小视频），不重命名不搬运——
        # 电影本来就落 MOVIE 库，正片/字幕保留原名（含画质组名信息）。
        return "plain", None
    if tag in ("raw", "meta"):
        return "plain", None
    parts = rel.replace(os.sep, "/").split("/")
    show_dir = parts[0]
    inter = parts[1:-1]                      # 中间子目录（季目录等）
    fname = parts[-1]
    stem, ext = os.path.splitext(fname)
    title = show.get("title") or show_dir
    season = season_from_path("/".join(parts[:-1]))

    if tag == "main":
        ep, ver = parse_episode(stem)
        if ep is None:
            return "plain", None
        name = "%s S%02dE%02d%s%s" % (title, season, ep, ("v%d" % ver) if ver > 1 else "", ext)
        return "dst", "/".join([dst_base, folder, show_dir] + inter + [name])

    if tag == "sub":
        ep, ver = parse_episode(stem)
        if ep is None:
            return "plain", None
        name = "%s S%02dE%02d%s.%s%s" % (title, season, ep, ("v%d" % ver) if ver > 1 else "",
                                          _sub_lang(fname), ext)
        return "dst", "/".join([dst_base, folder, show_dir] + inter + [name])

    if tag == "ova":
        if not show.get("ova_dir", True):
            return "plain", None
        return "dst", "/".join([dst_base, folder, show_dir, "OVA", fname])

    if tag == "movie":
        mode = (show.get("theater") or "route").lower()
        if mode == "skip":
            return "skip", None
        if mode == "keep":
            return "plain", None
        # route：下载即搬——直接落电影库（同一挂载卷，等价于"从下载就搬"）
        inner = stem if (_has_cjk(stem) or len(stem) >= 6) else ("%s 剧场版 (%s)" % (title, stem))
        return "dst", "/".join([dst_base, movie_dirname, "%s 剧场版" % title, inner + ext])

    return "plain", None


# -----------------------------------------------------------------------------
# 任务类
# -----------------------------------------------------------------------------

class Task:
    def __init__(self, folder, rel, src_size=0):
        self.folder = folder
        self.rel = rel
        self.id = f"{folder}/{rel}"
        self.src_size = src_size
        self.done = 0
        self.progress = 0
        self.speed = "-"
        self.eta = "-"
        self.state = "pending"   # pending / running / paused / done / error
        self.proc = None
        self.output = []
        self.last_update = 0
        self.retry = 0
        self.stall = 0              # 连续停滞次数（watchdog 用）
        self.miss_scans = 0         # 连续几轮扫描没看到源文件（僵尸任务清理用，v1.2.12）
        self.started_at = 0         # 最近一次启动时间
        self.last_logged_pct = -1   # 最近一次打日志的进度百分比
        self.last_beat_done = 0     # 上次心跳时的 done 字节数（用于计算真实下载速度）
        self.last_beat_time = 0     # 上次心跳时间
        self.keep = None            # 删源保护原因（v1.2.16）：非空=该源受保护不删
        self.dst = None             # v1.4 分流重定向：非空=下载到该绝对路径（改名/剧场版搬家/OVA归集）
        self.note = ""              # v1.4 分流备注（如"已过滤"），进状态快照便于排查

    def to_dict(self):
        return {
            "id": self.id,
            "folder": self.folder,
            "rel": self.rel,
            "src_size": self.src_size,
            "done": self.done,
            "progress": self.progress,
            "speed": self.speed,
            "eta": self.eta,
            "state": self.state,
            "keep": self.keep,
            "dst": self.dst or "",
            "note": self.note,
        }

# -----------------------------------------------------------------------------
# 同步引擎
# -----------------------------------------------------------------------------

class SyncEngine:
    def __init__(self):
        os.makedirs(STATE_DIR, exist_ok=True)
        os.makedirs(DST_BASE, exist_ok=True)
        self._init_state()

    def _init_state(self):
        """集中初始化全部运行时状态。

        ⚠️ 测试务必用 `SyncEngine.__new__(SyncEngine)` + 本方法构造实例，
        不要再手工逐个赋属性——手工列字段会在每次新增状态时与真实 __init__
        产生漂移，症状是 AttributeError 在 _build_status()/状态落盘路径里
        被 try 静默吞掉、测试却继续跑（v1.2.10 的锁类型、v1.2.16 的
        keep_entries 两次同因）。字段只在这里维护一份。
        """
        # RLock（可重入）：handle_control 持锁期间会调用 _save_status_snapshot()
        # → _build_status() 再次取锁。普通 Lock 在此死锁（v1.2.9 曾引入：
        # 立即落盘修复导致 HTTP 控制请求永久挂起，被 save_status_loop 的
        # 2 秒落盘兜底 + 前端轮询掩盖）。RLock 允许同一线程重入，一行修复。
        self.lock = threading.RLock()
        self.tasks = {}          # id -> Task
        self.priority = []       # 优先级队列（task id）：网页点"启动/重启"的任务插队，v1.2.12
        self.global_state = "running"   # running / paused
        self.folder_stats = []
        self.running = True
        # ---- 删源保护状态（v1.2.16）----
        self.keep_entries = []          # 外部保留名单里的路径前缀（归一化，无尾斜杠）
        self.keep_reasons = {}          # 前缀 -> 保护原因（用于日志/前端展示）
        self.keep_file_mtime = 0        # 名单文件最近一次读入时的 mtime（变了才重读）
        self.keep_file_broken = False   # 名单文件存在但解析失败 → 暂停删源（安全兜底）
        self.keep_broken_logged = False # 损坏告警只刷一次，避免刷屏
        self.keep_logged = {}           # (目录key, 原因) -> 上次打印时间（保留日志限频）
        self.activity = self._load_activity()   # {目录key: 最后活跃时间戳}
        self.dir_fingerprint = {}       # {目录key: 本轮内容指纹}（与上轮比对判断"还在更新"）
        self.dir_mtime = {}             # {目录key: 源文件最大 mtime}（每轮 scan 重建）
        self.dir_active = {}            # {目录key: max(mtime, activity)}（每轮 scan 重建，判定用）
        self.scanned_files = set()      # 本轮扫到的源文件 id（补删逻辑复用，避免二次遍历 FUSE）
        # ---- v1.4 分流规格 ----
        self.show_spec = {}             # {"DONGMAN/某番": {...元数据}}
        self.spec_cfg = {}              # thresholds 全局覆盖
        self.spec_mtime = 0.0
        self.spec_broken_logged = False
        self._spec_skip_logged = {}     # {tid: 上次打印时间}（skip 日志限频）
        self._kick = False              # v1.2.17 立即巡检标志
        self._kick_poll = 0             # v1.5 kick 轮询剩余次数（>0 时空扫则 30s 后重扫）
        self._load_show_spec(initial=True)

    # ---------------------------- 分流规格加载（v1.4）-------------------------
    def _load_show_spec(self, initial=False):
        """热加载 filter_shows.json（编排产出）。mtime 没变就跳过。
        文件不存在/解析失败 = 无分流（全部走传统行为），绝不因元数据问题停下载。"""
        if not SHOW_SPEC_FILE:
            return
        try:
            mtime = os.path.getmtime(SHOW_SPEC_FILE)
        except Exception:
            return
        if not initial and mtime == self.spec_mtime:
            return
        try:
            with open(SHOW_SPEC_FILE, "r", encoding="utf-8") as f:
                d = json.load(f)
            shows = d.get("shows") or {}
            self.show_spec = {str(k): v for k, v in shows.items() if isinstance(v, dict)}
            self.spec_cfg = (d.get("thresholds") or {}) if isinstance(d.get("thresholds"), dict) else {}
            self.spec_mtime = mtime
            self.spec_broken_logged = False
            log(f"  [分流规格] 已加载 {len(self.show_spec)} 个条目（{os.path.basename(SHOW_SPEC_FILE)}）")
            self._apply_spec_to_pending()
        except Exception as e:
            # 规格坏了不影响下载：沿用旧规格，告警一次
            if not self.spec_broken_logged:
                log(f"  [警告] 分流规格解析失败，沿用旧规格: {e}")
                self.spec_broken_logged = True

    def _apply_spec_to_pending(self):
        """规格变更后同步存量 pending 任务：该跳过的不再排队，该重定向的改 dst。
        running/done 不动（下载中的打断反而浪费）。"""
        n_skip, n_move = 0, 0
        with self.lock:
            for tid, t in list(self.tasks.items()):
                if t.state != "pending":
                    continue
                action, dst = self._spec_decision(t.folder, t.rel, t.src_size)
                if action == "skip":
                    t.state = "done"
                    t.progress = 100
                    t.eta = "已过滤"
                    t.note = "分流过滤：不需要下载"
                    n_skip += 1
                elif action == "dst" and dst != t.dst:
                    t.dst = dst
                    n_move += 1
        if n_skip or n_move:
            log(f"  [分流规格] 存量任务同步：跳过 {n_skip} / 重定向 {n_move}")

    def _spec_decision(self, folder, rel, size, dir_median=0, ep_counts=None):
        """查分流规格，返回 (action, dst)。action ∈ skip/plain/dst。
        show 键 = "FOLDER/一级子目录"；目录不在名单 → plain（传统行为）。"""
        rel_norm = rel.replace(os.sep, "/")
        top = rel_norm.split("/", 1)[0]
        show = self.show_spec.get(f"{folder}/{top}")
        if show is None:
            return "plain", None
        return build_task_dst(folder, rel_norm, size, dir_median, show, DST_BASE, MOVIE_DIRNAME,
                              ep_counts=ep_counts)

    # ---------------------------- 删源保护（v1.2.16）-------------------------
    # 设计要点：
    #  · 判定单元是"番剧目录" = FOLDER/一级子目录，而不是单个文件——
    #    "这一集下完了"不等于"这部番完结了"，必须看整个目录还在不在冒新文件。
    #  · 活跃度取两路信号的较近者：
    #      信号A 源文件 mtime（网盘/挂载层给的时间，可能不准或缺失）
    #      信号B 引擎自观测（目录内容指纹变了就记 now）——自建、绝对可靠
    #    两者取 max，任一路说"最近有动静"就按"还在更新"处理（保守）。
    #  · 判不出活跃度时一律保留源：删源不可逆，信息不足就不动手。
    def _load_activity(self):
        try:
            with open(ACTIVITY_FILE, "r", encoding="utf-8") as f:
                d = json.load(f)
            if isinstance(d, dict):
                return {str(k): float(v) for k, v in d.get("dirs", {}).items()}
        except Exception:
            pass
        return {}

    def _save_activity(self):
        try:
            d = os.path.dirname(ACTIVITY_FILE)
            if d:
                os.makedirs(d, exist_ok=True)
            tmp = ACTIVITY_FILE + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump({"updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                           "dirs": self.activity}, f, ensure_ascii=False)
            os.replace(tmp, ACTIVITY_FILE)
        except Exception as e:
            log(f"  [警告] 活跃度记录写入失败: {e}")

    def _load_keep_entries(self, force=False):
        """热加载外部保留名单。mtime 没变就跳过（每轮巡检调用，几乎零成本）。
        返回 True = 名单可用。文件不存在不算"坏"，只是没有名单层保护。"""
        if not KEEP_SRC_FILE:
            return False
        try:
            mtime = os.path.getmtime(KEEP_SRC_FILE)
        except Exception:
            # 名单突然读不到（挂载抖动/被删）：已加载的旧名单继续生效，不改变行为
            return bool(self.keep_entries)
        if not force and mtime == self.keep_file_mtime:
            return True
        try:
            with open(KEEP_SRC_FILE, "r", encoding="utf-8") as f:
                d = json.load(f)
            raw = d.get("keep", []) if isinstance(d, dict) else (d if isinstance(d, list) else [])
            entries, reasons = [], {}
            for item in raw or []:
                if isinstance(item, str) and item.strip():
                    p = item.strip().strip("/")
                    entries.append(p)
                    reasons[p] = ""
                elif isinstance(item, dict) and item.get("path"):
                    p = str(item["path"]).strip().strip("/")
                    entries.append(p)
                    reasons[p] = str(item.get("reason") or "")
            reason_map = {}
            if isinstance(d, dict):
                # 键也要归一化：生产文件里 reason 的键是 "/DONGMAN/某番"（带前导斜杠），
                # 而 entries 走的是 strip("/") 后的 "DONGMAN/某番"。不对齐就永远取不到，
                # 日志/前端只会显示兜底的"外部保留名单"——保护生效了但看不出为什么。
                for k, v in (d.get("reason") or {}).items():
                    reason_map[str(k).strip().strip("/")] = str(v)
            for p in entries:
                if reason_map.get(p):
                    reasons[p] = reason_map[p]
            self.keep_entries = entries
            self.keep_reasons = reasons
            self.keep_file_mtime = mtime
            self.keep_file_broken = False
            self.keep_broken_logged = False
            log(f"  [保留名单] 已加载 {len(entries)} 条保护规则（{KEEP_SRC_FILE}）")
            return True
        except Exception as e:
            # 名单坏了 → 冻结删源。绝不能把"读坏了"当成"没有保护项"。
            self.keep_file_broken = True
            if not self.keep_broken_logged:
                log(f"  [严重] 保留名单解析失败，已暂停全部删源以防误删: {e}")
                self.keep_broken_logged = True
            return False

    def _match_keep(self, path):
        """路径前缀匹配：keep 里写 DONGMAN/葬送，可命中 DONGMAN/葬送/xxx.mkv。
        按路径段比较，避免 "DONGMAN/葬送" 误命中 "DONGMAN/葬送的芙莉莲2"。"""
        p = path.strip("/")
        for e in self.keep_entries:
            if p == e or p.startswith(e + "/"):
                return e
        return None

    def _dir_key(self, folder, rel):
        """取番剧目录 key（FOLDER/一级子目录）。文件直接躺在父目录下时返回 None。"""
        rel = rel.replace("\\", "/").strip("/")
        if not rel or "/" not in rel:
            return None
        return f"{folder}/{rel.split('/', 1)[0]}"

    def _dir_last_active(self, folder, rel):
        """目录最后活跃时间 = max(源文件最大 mtime, 引擎自观测的最近变更时间)。
        文件直接躺在 FOLDERS 根下时没有"番剧目录"可看，退化为看它自己的 mtime。"""
        key = self._dir_key(folder, rel)
        if not key:
            try:
                return os.path.getmtime(self.src_path(folder, rel))
            except Exception:
                return 0.0
        return max(self.dir_mtime.get(key, 0.0), self.activity.get(key, 0.0))

    def _note_dir_activity(self, folder, subdir, rels):
        """scan() 每轮对每个一级子目录调用。指纹变了 = 目录里冒了新文件 →
        把活跃时间刷成 now。这是"还在更新"的最直接证据，不依赖任何外部元数据。
        首次见到的目录：以目录 mtime 为基线（拿不到就用 now，等价于从此刻重新计时）。"""
        key = f"{folder}/{subdir}"
        sig = "|".join(sorted(rels))
        prev = self.dir_fingerprint.get(key)
        self.dir_fingerprint[key] = sig
        if prev is None:
            if key not in self.activity:
                self.activity[key] = self.dir_mtime.get(key, 0.0) or time.time()
            return
        if sig != prev:
            self.activity[key] = time.time()

    def check_keep(self, folder, rel):
        """删源前的总闸门。返回 (keep, reason)；keep=True 表示这个源不许删。"""
        path = f"{folder}/{rel}".replace("\\", "/")
        self._load_keep_entries()
        if self.keep_file_broken:
            return True, "保留名单损坏·已暂停删源"
        hit = self._match_keep(path)
        if hit:
            why = self.keep_reasons.get(hit) or "外部保留名单"
            return True, why
        if DELETE_SRC_QUIET_DAYS <= 0:
            return False, ""
        if DELETE_SRC_QUIET_FOLDERS and folder not in DELETE_SRC_QUIET_FOLDERS:
            return False, ""
        last = self._dir_last_active(folder, rel)
        if last <= 0:
            return True, "无法判定活跃度·保守保留"
        idle = (time.time() - last) / 86400.0
        if idle < DELETE_SRC_QUIET_DAYS:
            return True, f"静默 {idle:.1f}/{DELETE_SRC_QUIET_DAYS} 天·仍在更新"
        return False, ""

    def _log_keep(self, tid, reason, max_age=1800):
        """保留日志限频：同一目标同一原因半小时内只打一次（每轮巡检都会跑到）。"""
        now = time.time()
        k = f"{tid}|{reason}"
        if now - self.keep_logged.get(k, 0) < max_age:
            return
        self.keep_logged[k] = now
        log(f"  [保留源·{reason}] {tid}")

    # ---------------------------- 文件大小 / 路径 ----------------------------
    def src_path(self, folder, rel=""):
        return os.path.join(SRC_BASE, folder, rel)

    def dst_path(self, folder, rel=""):
        return os.path.join(DST_BASE, folder, rel)

    def file_size(self, path):
        try:
            return os.path.getsize(path)
        except Exception:
            # 读取失败返回 -1（而非 0），与"真实的 0 字节文件"区分，
            # 避免删源判定里把"读不到大小"误判成"大小一致"而误删网盘源文件。
            return -1

    # ---------------------------- 扫描 & 任务管理 ----------------------------
    def _dst_has_same_size(self, dst_now, src_size, index):
        """dst 同目录下是否已存在同字节数文件（v1.6.1 预检改名兜底）。

        index: {目录: {字节数: 文件名}}，由 scan 每轮传入缓存，避免 FUSE 反复 listdir。
        """
        ddir = os.path.dirname(dst_now)
        if not ddir:
            return False
        m = index.get(ddir)
        if m is None:
            m = {}
            try:
                for n in os.listdir(ddir):
                    sz = self.file_size(os.path.join(ddir, n))
                    if sz > 0:
                        m.setdefault(sz, n)
            except Exception:
                pass
            index[ddir] = m
        return src_size in m

    def scan(self):
        """扫描源目录，把缺失/未下完的文件加入 pending 队列"""
        self.scanned_files = set()
        self._load_show_spec()   # v1.4：每轮热加载分流规格（mtime 变了才重读）
        dst_size_index = {}      # v1.6.1 预检改名兜底的目录尺寸索引（每轮缓存）
        for folder in FOLDERS:
            src_root = self.src_path(folder)
            if not os.path.isdir(src_root):
                log(f"源目录不存在，跳过: {src_root}")
                continue
            # 刷新 FUSE 缓存
            try:
                os.listdir(src_root)
                list(Path(src_root).rglob("*"))
            except Exception:
                pass
            seen = set()
            dir_files = {}   # 一级子目录 -> [相对路径]（v1.2.16 目录活跃度观测）
            for root, dirs, files in os.walk(src_root):
                if root == src_root:
                    # 一级子目录首次建基线时取一次目录 mtime（拿不到就是 0）；
                    # 此后只靠下面的"内容指纹"观测，不再对每个文件 stat，避免拖慢 FUSE。
                    for dn in dirs:
                        k = f"{folder}/{dn}"
                        if k not in self.dir_mtime:
                            try:
                                self.dir_mtime[k] = os.path.getmtime(os.path.join(root, dn))
                            except Exception:
                                self.dir_mtime[k] = 0.0
                # v1.4：本目录视频大小中位数（广告判定基准：正片大小是聚类的）。
                # 只对"该目录有条目在分流名单"时才有意义，但统一算也不贵（每轮一次 stat）。
                _vsizes = []
                for _n in files:
                    if os.path.splitext(_n)[1].lower() in VIDEO_EXTS:
                        _sz = self.file_size(os.path.join(root, _n))
                        if _sz > 0:
                            _vsizes.append(_sz)
                dir_median = sorted(_vsizes)[len(_vsizes) // 2] if _vsizes else 0
                # v1.6.1 唯一集数豁免的统计依据：本目录 {集数: 文件数}
                _dir_ep_counts = {}
                for _n in files:
                    if os.path.splitext(_n)[1].lower() in VIDEO_EXTS:
                        _ep, _ = parse_episode(_n)
                        if _ep is not None:
                            _dir_ep_counts[_ep] = _dir_ep_counts.get(_ep, 0) + 1
                for name in files:
                    full_src = os.path.join(root, name)
                    rel = os.path.relpath(full_src, src_root)
                    tid = f"{folder}/{rel}"
                    seen.add(tid)
                    rel_norm = rel.replace(os.sep, "/")
                    if "/" in rel_norm:
                        dir_files.setdefault(rel_norm.split("/", 1)[0], []).append(rel_norm)
                    if ".rsync-partial" in full_src:
                        continue
                    with self.lock:
                        if tid in self.tasks:
                            t = self.tasks[tid]
                            if t.state in ("done", "running"):
                                continue
                            # pending/error/paused：复核目标文件是否已实际下满。
                            # rsync 可能早已写完数据但卡死在 WebDAV 收尾（close/flush）
                            # 导致 proc.wait() 挂起、rc==0 分支永不执行、任务永远 running——
                            # 表现为"只有重启容器任务才完成"。这里用目标大小兜底纠偏。
                            if t.src_size > 0:
                                d_sz = self.file_size(t.dst or self.dst_path(t.folder, t.rel))
                                if d_sz >= t.src_size:
                                    t.state = "done"
                                    t.progress = 100
                                    t.done = t.src_size
                                    t.speed = "-"
                                    t.eta = "完成"
                                    t.last_update = time.time()
                                    log(f"[{t.id}] 目标已下满({fmt_size(d_sz)}/{fmt_size(t.src_size)})，直接标记完成（rsync 收尾卡死已兜底）")
                                    self._cleanup_src_file(t)
                                    continue
                            # pending/paused/error 且未下满：保留，不重置
                            continue
                        src_size = self.file_size(full_src)
                        if src_size < 0:
                            # 读取大小失败（FUSE/WebDAV 抖动），本轮先不收，下轮巡检再试
                            log(f"  [跳过-读取大小失败] {tid}，下轮再扫描")
                            continue
                        # v1.4 分流决策：该目录的条目在规格名单里才生效（否则传统行为）。
                        # dir_median 在 walk 循环顶部按目录算好（广告大小判定基准）。
                        action, dst = self._spec_decision(folder, rel_norm, src_size, dir_median,
                                                          _dir_ep_counts)
                        if action == "skip":
                            now = time.time()
                            if now - self._spec_skip_logged.get(tid, 0) > 3600:
                                self._spec_skip_logged[tid] = now
                                log(f"  [分流] 跳过 {tid}（过滤：不需要下载）")
                            continue
                        t = Task(folder, rel, src_size)
                        if action == "dst":
                            t.dst = dst
                            log(f"  [分流] {tid} → {dst}")
                        # v1.6 建任务预检：目标文件已存在且尺寸达标 → 直接落 done，不入队。
                        # 引擎任务表在内存（STATE_DIR tmpfs），重启即清零；已下满的文件
                        # 重启后会被重新扫成 pending 占队列（rsync --size-only 虽不传流量，
                        # 但要排队等并发槽、制造"假性重复下载"观感）。预检在入队前拦截。
                        _dst_now = t.dst or self.dst_path(t.folder, t.rel)
                        _d_sz = self.file_size(_dst_now)
                        if src_size > 0 and _d_sz < src_size:
                            # 改名兜底（v1.6.1）：TMDB 命名/订阅退场后 spec 消失会让任务
                            # 回落 plain 名，但同目录里改名版正片其实已在（字节一致）。
                            # 同目录存在同字节数文件即视为已下载——与 rsync --size-only
                            # 判定语义一致，否则会真下一遍 plain 名副本（FA 64 集≈20GB）。
                            if self._dst_has_same_size(_dst_now, src_size, dst_size_index):
                                _d_sz = src_size
                        if src_size > 0 and _d_sz >= src_size:
                            t.state = "done"
                            t.progress = 100
                            t.done = src_size
                            t.eta = "完成"
                            t.last_update = time.time()
                            t.note = "预检:目标已满,未入队"
                            self.tasks[tid] = t
                            log(f"  [预检] {tid} 目标已存在({fmt_size(max(_d_sz, 0))}/{fmt_size(src_size)})，直接标记完成（不入队）")
                            self._cleanup_src_file(t)
                            continue
                        self.tasks[tid] = t
            # 目录活跃度观测（v1.2.16）：指纹变了 = 目录里冒了新文件 = 还在更新
            for sub, rels in dir_files.items():
                self._note_dir_activity(folder, sub, rels)
            self.scanned_files |= seen
            # 本轮没扫到源文件的任务 → 自动清理（v1.2.12 僵尸任务清理）
            self._prune_missing_tasks(folder, seen)
            # 已确定无用的 rsync 孤儿临时文件 → 清扫（v1.2.13）
            self._cleanup_rsync_temps(folder)

    def _prune_missing_tasks(self, folder, seen):
        """源文件已被删除（如用户在 AList/网盘里删了文件）时，自动移除对应任务记录。

        旧行为：源文件没了任务记录还挂着，网页端永远显示"失败/等待"且无法清除。
        规则（保守，防误清）：
        - 连续 2 轮扫描（约 20 分钟）都看不到源文件才移除——单轮缺失可能只是
          FUSE/WebDAV 抖动，不能作为删除依据；
        - running 任务不动：等 rsync 自己失败转 error 后，下轮再按本规则清；
        - done 任务不动：由主循环"完成 1 小时后清理"负责，避免与 DELETE_SRC
          正常删源混淆刷日志。"""
        with self.lock:
            for tid in list(self.tasks.keys()):
                t = self.tasks[tid]
                if t.folder != folder or t.state in ("running", "done"):
                    continue
                if tid in seen:
                    t.miss_scans = 0
                    continue
                t.miss_scans = getattr(t, "miss_scans", 0) + 1
                if t.miss_scans >= 2:
                    del self.tasks[tid]
                    log(f"[{tid}] 源文件已不存在（连续 2 轮未扫到），自动移除任务记录")

    def _cleanup_rsync_temps(self, folder):
        """清扫已确定无用的 rsync 孤儿临时文件（v1.2.13）。

        孤儿临时文件（.<文件名>.6位随机，rsync 被硬杀的遗留）分两种结局：
        - 任务还要下载（pending/paused/error）→ 留着，_salvage_rsync_temp()
          会在任务启动时把它抢救为断点；
        - 文件已完整落地（done）或任务与网盘源都已不存在 → 彻底无用，删除。
        保守条件：mtime 超过 1 小时 + 严格匹配临时文件命名，不碰正常文件、
        不碰 60 分钟内新产生的（可能是刚中断、还没轮到任务重启的断点）。"""
        dst_root = self.dst_path(folder)
        if not os.path.isdir(dst_root):
            return
        pat = re.compile(r"^\.(.+)\.([A-Za-z0-9]{6})$")
        now = time.time()
        for root, dirs, files in os.walk(dst_root):
            dirs[:] = [d for d in dirs if d != ".rsync-partial"]   # 断点目录交给 rsync 自己管
            for name in files:
                m = pat.match(name)
                if not m:
                    continue
                full = os.path.join(root, name)
                try:
                    if now - os.path.getmtime(full) < 3600:
                        continue
                except Exception:
                    continue
                rel = os.path.relpath(full, dst_root).replace(os.sep, "/")
                rel_dir = rel.rsplit("/", 1)[0] if "/" in rel else ""
                real_rel = f"{rel_dir}/{m.group(1)}" if rel_dir else m.group(1)
                tid = f"{folder}/{real_rel}"
                t = self.tasks.get(tid)
                if t is not None and t.state == "done":
                    reason = "文件已完整落地，临时文件无用"
                elif t is None and not os.path.exists(self.src_path(folder, real_rel)):
                    reason = "任务与网盘源文件均已不存在"
                else:
                    continue   # 任务还会下载 → 留给断点抢救
                try:
                    sz = os.path.getsize(full)
                    os.remove(full)
                    log(f"[{tid}] 清理孤儿临时文件 {name}({fmt_size(sz)})：{reason}")
                except Exception:
                    pass

    # ---------------------------- fnOS mountmgr 远程缓存刷新 ----------------------------
    def _find_pids_by_name(self, name):
        """在 /proc 里按进程名找 PID（容器需 pid: host 才能看到宿主机进程）"""
        pids = []
        if not os.path.isdir("/proc"):
            return pids
        for entry in os.listdir("/proc"):
            if not entry.isdigit():
                continue
            try:
                with open(f"/proc/{entry}/comm", "r", encoding="utf-8") as f:
                    comm = f.read().strip()
                if comm == name:
                    pids.append(int(entry))
            except (FileNotFoundError, ProcessLookupError, PermissionError):
                pass
        return pids

    def refresh_remote_cache(self):
        """fnOS环境：给真正持有 FUSE 挂载的进程发 SIGHUP，触发其内部状态/挂载刷新。

        fnOS的夸克/网盘远程挂载由 `rclone rcd` 承载（`/proc/PID/comm` 是 `rclone`，
        `rcd` 是其子命令；`/etc/mountmgr/` 是fnOS内部的配置目录名，不是进程名）。
        默认会先试 mountmgr（极老版本兼容），再试 rclone、rcd。
        容器需配 `pid: host` 才能看到宿主机进程。
        非fnOS环境 / 找不到目标进程时函数 no-op，不影响主流程；
        找不到的告警每小时最多打一次，避免每轮巡检刷屏。

        注意：rclone 收到 SIGHUP 默认会退出重载（systemd/no-systemd 行为不同）。
        fnOS上 rclone 一般由系统守护，挂掉会自动拉起；偶有秒级中断属正常。
        如发现 SIGHUP 后挂载长时间未恢复，可改用 rclone rc API（unix socket
        `/var/run/rclone/rcd_*.sock`）调用缓存清理——需要再实现。
        """
        now = time.time()
        for name in PROC_NAMES:
            pids = self._find_pids_by_name(name)
            if not pids:
                continue
            sent = 0
            for pid in pids:
                try:
                    os.kill(pid, 1)  # SIGHUP
                    sent += 1
                except (ProcessLookupError, PermissionError) as e:
                    log(f"发 SIGHUP 给 {name}(PID {pid}) 失败: {e}")
            if sent:
                log(f"已给 {sent} 个 {name} 进程发 SIGHUP 触发 FUSE 缓存刷新")
                self._last_cache_warn = now
                return
        # 都没找到：非fnOS / pid namespace 隔离环境，no-op
        last = getattr(self, "_last_cache_warn", 0)
        if now - last >= 3600:
            self._last_cache_warn = now
            log(f"refresh_remote_cache: 未找到目标进程 {PROC_NAMES}"
                f"（需 compose 配 pid: host；若已配，请用 PROC_NAMES 指定宿主机真实进程名，如 rclone）")

    # ---------------------------- 启动任务 ----------------------------
    def start_task(self, t):
        if t.dst:
            # v1.4 分流任务：单文件 rsync 到指定路径（支持改名/跨目录搬家）。
            # --partial-dir 放目标目录，断点续传与普通任务同机制。
            dst_dir = os.path.dirname(t.dst)
            os.makedirs(dst_dir, exist_ok=True)
            self._salvage_rsync_temp(t)
            src_arg = f"{SRC_BASE}/{t.folder}/{t.rel}"
            partial_dir = os.path.join(dst_dir, ".rsync-partial")
            cmd = [
                "rsync", "-aP", "--size-only",
                "--timeout=120",
                f"--partial-dir={partial_dir}",
                src_arg, t.dst
            ]
            log(f"[{t.id}] 开始下载 → {t.dst}")
        else:
            src = self.src_path(t.folder, t.rel)
            dst = self.dst_path(t.folder)
            os.makedirs(os.path.dirname(self.dst_path(t.folder, t.rel)), exist_ok=True)
            self._salvage_rsync_temp(t)   # v1.2.13：先把历史硬中断的孤儿临时文件抢救为断点
            # 使用 rsync -R 保留相对路径：src/./folder/rel -> dst/folder/rel
            src_arg = f"{SRC_BASE}/./{t.folder}/{t.rel}"
            partial_dir = os.path.join(dst, ".rsync-partial")
            # --timeout=120：网络层兜底超时，配合 --partial 中断可续传
            cmd = [
                "rsync", "-aP", "--size-only",
                "--timeout=120",
                f"--partial-dir={partial_dir}",
                "-R", src_arg, f"{DST_BASE}/"
            ]
            log(f"[{t.id}] 开始下载")
        t.state = "running"
        t.started_at = time.time()
        t.last_update = time.time()
        proc = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, bufsize=1
        )
        t.proc = proc
        # 重置心跳基线：本次启动后的字节增量才计入"真实下载速度"
        t.last_beat_done = t.done
        t.last_beat_time = time.time()
        threading.Thread(target=self._reader, args=(t, proc), daemon=True).start()

    def _reader(self, t, proc):
        """读取 rsync 输出并解析进度。

        proc 是启动时的快照：任务被重启（watchdog / 手动 restart）后 t.proc 会被替换，
        本线程只认自己启动的那个进程，避免读错 stdout 或把新任务状态覆盖掉。
        """
        try:
            for line in proc.stdout:
                line = line.rstrip()
                if not line:
                    continue
                t.output.append(line)
                if len(t.output) > 200:
                    t.output.pop(0)
                # rsync 进度行示例：  32,768,000  45%   45.67MB/s    0:00:15
                # 注意：数字带千分位逗号；速度单位形如 MB/s / KB/s / GB/s（双字符），须整体匹配
                m = re.search(
                    r"([\d,]+(?:\.\d+)?[BMKGT]?)\s+(\d+)%\s+([\d,]+(?:\.\d+)?[BMKGT]?B/s)\s+(\d+:\d+(?::\d+)?)",
                    line, re.I
                )
                if m:
                    done_str, pct, speed, eta = m.groups()
                    pct = int(pct)
                    with self.lock:
                        if t.proc is not proc:   # 任务已被新进程接管，本线程退出
                            return
                        t.done = parse_size(done_str)
                        t.progress = pct
                        t.speed = speed
                        t.eta = fmt_eta(eta)
                        t.last_update = time.time()
                        # 每跨 10% 打一次进度日志：进度/大小/速度/剩余时间
                        if pct - t.last_logged_pct >= 10:
                            t.last_logged_pct = pct // 10 * 10
                            log(f"[{t.id}] {t.progress}% | {fmt_size(t.done)}/{fmt_size(t.src_size)} | {t.speed} | 剩余{t.eta}")
        except Exception as e:
            log(f"[{t.id}] 读取输出异常: {e}")
        # wait 带超时：rsync 若卡死在 WebDAV 收尾（close/flush），wait() 会永久挂起，
        # 任务永远 running 占槽。wait(timeout) 超时后直接放弃本线程，
        # 由 watchdog 的"目标已下满"兜底逻辑负责 kill + 标记完成。
        try:
            rc = proc.wait(timeout=60)
        except subprocess.TimeoutExpired:
            log(f"[{t.id}] rsync 进程 60s 未退出（疑似卡死在挂载收尾），交给 watchdog 兜底")
            try:
                proc.kill()
            except Exception:
                pass
            return
        with self.lock:
            if t.proc is not proc:   # 已被新进程接管（重启/续传），别覆盖新状态
                return
            if t.state == "paused":
                return
            if rc == 0:
                t.state = "done"
                t.progress = 100
                t.speed = "-"
                t.eta = "完成"
                t.done = t.src_size
                t.last_update = time.time()
                log(f"[{t.id}] 下载完成 ✓ ({fmt_size(t.src_size)})")
                self._cleanup_src_file(t)
            else:
                if t.state == "pending":
                    # watchdog 已接管为重启，不再覆盖状态
                    return
                t.state = "error"
                t.speed = "-"
                log(f"[{t.id}] 下载失败/中断 (rc={rc})")

    # ---------------------------- 调度 ----------------------------
    def scheduler(self):
        """所有 sleep 必须在 with self.lock 之外，避免把 self.lock 占用 0.5/1 秒
        导致 /control /status 请求被串行卡住几秒——用户实测 handle_control 拿到
        "HTTP 控制请求" 日志后 6 秒才进 start_file 把 state 改掉。"""
        while self.running:
            skipped_sleep = 0.0
            dispatched = False
            with self.lock:
                # 检查全局暂停
                if self.global_state == "paused":
                    skipped_sleep = 1.0
                else:
                    # 统计 running
                    running = [t for t in self.tasks.values() if t.state == "running"]
                    if len(running) >= CONCURRENT:
                        # 满了也别 sleep 太久，0.1 秒后回来再判断（任何 running 一
                        # 旦 done/error 立即腾位）。锁里 sleep 会卡住 API。
                        skipped_sleep = 0.1
                    else:
                        # 找下一个 pending：优先级队列（网页点"启动/重启"插队的任务）优先，v1.2.12
                        next_task = None
                        while self.priority:
                            ptid = self.priority.pop(0)
                            pt = self.tasks.get(ptid)
                            if pt and pt.state == "pending":
                                next_task = pt
                                break
                            # 任务已被移除/已完成/已被启动 → 丢弃，继续找下一个
                        if not next_task:
                            for t in self.tasks.values():
                                if t.state == "pending":
                                    next_task = t
                                    break
                        if next_task:
                            self.start_task(next_task)
                            dispatched = True
            # 锁外 sleep，不阻塞 API
            if skipped_sleep:
                time.sleep(skipped_sleep)
            else:
                # 有任务在调度也降频，避免频繁 Popen
                time.sleep(0.5 if dispatched else 0.1)

    # ---------------------------- 停滞自愈 + 心跳日志 ----------------------------
    def watchdog_loop(self):
        """每 10 秒检查一次：

        1) 停滞检测：running 任务超过 STALL_SECONDS 没有任何进度（字节不推进，
           即 rsync 卡在 FUSE 读取/网盘限流）→ 杀掉 rsync 自动重启，实现"低速度/无速度自愈"。
           连续停滞 STALL_RETRY 次仍无进展 → 置 error 等下一轮巡检，避免空转。
           关键：卡死通常是**挂载点级**（夸克连接池占满/限流），只杀 rsync 重启还是会读
           同一个卡死的 FUSE 挂载点（帕金森循环）。因此连续停滞达到 STALL_MOUNT_RESET 次时，
           额外给挂载进程（PROC_NAMES，如 rclone/alist）发 SIGHUP 重置挂载连接——这正是
           手动 `docker restart` 能解冻的原因（容器重启会重建 rclone 挂载）。配 pid: host 才能生效。
        2) 心跳汇总：每 HEARTBEAT_SECONDS 打印一行当前正在下载的任务（谁在下、进度、速度）。
        """
        last_beat = time.time()
        while self.running:
            time.sleep(10)
            now = time.time()
            with self.lock:
                for t in list(self.tasks.values()):
                    if t.state != "running" or t.proc is None:
                        continue
                    # 兜底：rsync 卡死在收尾（close/flush）导致 proc.wait() 挂起时，
                    # 目标文件其实已下满。直接 kill 进程并标记完成，释放并发槽，
                    # 否则任务永远 running，后面的任务永远排不上（用户实测：重启容器才完成）。
                    if t.src_size > 0:
                        try:
                            d_sz = self.file_size(t.dst or self.dst_path(t.folder, t.rel))
                        except Exception:
                            d_sz = -1
                        if d_sz >= t.src_size:
                            try:
                                t.proc.kill()
                            except Exception:
                                pass
                            t.state = "done"
                            t.progress = 100
                            t.done = t.src_size
                            t.speed = "-"
                            t.eta = "完成"
                            t.last_update = now
                            log(f"[{t.id}] 目标已下满({fmt_size(d_sz)}/{fmt_size(t.src_size)})，rsync 卡在收尾，强制完成释放槽位")
                            self._cleanup_src_file(t)
                            continue
                    # 字节推进判定：不看 rsync 输出行时间（限速时输出行仍在刷，
                    # last_update 永远新鲜 → 停滞检测失效），只看 done 是否真的在涨。
                    dt = now - getattr(t, "_w_time", now)
                    if dt <= 0:
                        dt = 0.1
                    delta = max(0, (t.done or 0) - getattr(t, "_w_done", t.done or 0))
                    rate = delta / dt
                    t._w_time = now
                    t._w_done = t.done or 0
                    if rate >= LOW_SPEED_BPS:      # 速率正常 → 清零计数
                        t._w_stall = 0
                        t._w_low = 0
                    elif rate <= 0.0:               # 完全无推进 → 停滞
                        t._w_stall = getattr(t, "_w_stall", 0) + dt
                        t._w_low = 0
                    else:                           # 龟速推进 → 低速累计
                        t._w_low = getattr(t, "_w_low", 0) + dt
                        t._w_stall = 0
                    # ① 低速让位：单任务长期龟速（AList WebDAV 限速典型症状）占着槽位，
                    #    队列里其他任务永远排不上。让位给下一个任务，等下一轮巡检再试。
                    if t._w_low >= LOW_SPEED_SECONDS:
                        t._w_low = 0
                        try:
                            t.proc.terminate()
                            t.proc.wait(timeout=5)
                        except Exception:
                            try:
                                t.proc.kill()
                            except Exception:
                                pass
                        t.state = "error"
                        t.stall = 0
                        log(f"[{t.id}] 持续低速 {int(LOW_SPEED_SECONDS)}s（约 {fmt_speed(rate)}），让位给下一个任务，本轮暂停")
                        if PUSH_URL:
                            notify(f"低速让位: {t.id}")
                        continue
                    # ② 完全停滞：N 秒无任何字节推进 → 杀 rsync 重启
                    if t._w_stall < STALL_SECONDS:
                        continue
                    t.stall += 1
                    # 挂载点级卡死：只杀 rsync 重启仍读同一个卡死的 FUSE 挂载，等于手动
                    # docker restart 里"重启 rclone 挂载"那一步没做 —— 这正是用户实测"只有重启
                    # 容器才解冻"的原因。连续停滞达到阈值时，给 rclone/alist 发 SIGHUP 重置挂载
                    # 连接，把卡死的夸克连接池清空，下面的 rsync 重启才能真正读到数据。
                    if t.stall >= STALL_MOUNT_RESET:
                        log(f"[{t.id}] 连续停滞 {t.stall} 次（挂载点级卡死），重置挂载连接 SIGHUP {PROC_NAMES} 以解冻 FUSE 读取")
                        self.refresh_remote_cache()
                    try:
                        t.proc.terminate()
                    except Exception:
                        pass
                    if t.stall >= STALL_RETRY:
                        t.state = "error"
                        t.stall = 0
                        log(f"[{t.id}] 停滞 {int(t._w_stall)}s 无字节推进，已连续 {STALL_RETRY} 次无进展，本轮暂停等待下轮巡检")
                        if PUSH_URL:
                            notify(f"停滞放弃: {t.id}")
                    else:
                        t.state = "pending"
                        t.last_update = now   # 防止立刻再次命中
                        log(f"[{t.id}] 停滞 {int(t._w_stall)}s 无字节推进，已自动重启（第 {t.stall}/{STALL_RETRY} 次）")
                        if PUSH_URL:
                            notify(f"停滞重启: {t.id}")
            if now - last_beat >= HEARTBEAT_SECONDS:
                last_beat = now
                with self.lock:
                    running = [t for t in self.tasks.values() if t.state == "running"]
                    if running:
                        pend = sum(1 for t in self.tasks.values() if t.state == "pending")
                        parts = []
                        for t in running:
                            # 真实下载速度 = 两次心跳之间 done 的字节增量 / 时间增量
                            # （rsync 自报的 speed 是从 FUSE 缓存读数据的速度，不是云端到本地的真实速率）
                            dt = now - t.last_beat_time
                            delta = max(0, (t.done or 0) - (t.last_beat_done or 0))
                            real_bps = (delta / dt) if dt > 0 else 0
                            real_speed = fmt_speed(real_bps) if real_bps > 0 else "0B/s"
                            t.last_beat_done = t.done or 0
                            t.last_beat_time = now
                            # 距上次 rsync 进度行多久（>2 个心跳周期视为停滞）
                            idle = now - t.last_update
                            stall = f" 停滞{int(idle)}s" if idle > 2 * HEARTBEAT_SECONDS else ""
                            # rsync 报的速度作参考（标注避免与真实速度混淆）
                            rsync_hint = f" rsync报{t.speed}" if t.speed and t.speed != "-" else ""
                            parts.append(f"[{t.id}] {t.progress}% 真实{real_speed}{stall}{rsync_hint}")
                        line = " | ".join(parts)
                        log(f"当前下载({len(running)}条, 排队{pend}): {line}")

    # ---------------------------- 删源（本地完整才删） ----------------------------
    def _cleanup_src_file(self, t):
        if DELETE_SRC == "false":
            return
        src = self.src_path(t.folder, t.rel)
        dst = self.dst_path(t.folder, t.rel)
        if not os.path.exists(src) or not os.path.exists(dst):
            return
        s_sz = self.file_size(src)
        d_sz = self.file_size(dst)
        # 安全校验：任一侧大小异常(<=0，含读取失败返回的 -1)一律保留，不删源
        if s_sz <= 0 or d_sz <= 0:
            log(f"  [保留-大小异常] {t.id} (src={s_sz}, dst={d_sz})，跳过删源")
            return
        if s_sz != d_sz:
            return
        # 删源策略闸门（v1.2.16）：连载中 / 名单保护 / 判不出活跃度 → 保留源
        keep, why = self.check_keep(t.folder, t.rel)
        t.keep = why if keep else None
        if keep:
            self._log_keep(t.id, why)
            return
        if DELETE_SRC == "preview":
            log(f"  [预览-将删源] {t.id}")
            return
        try:
            os.remove(src)
            log(f"  [已删源] {t.id}")
        except Exception as e:
            log(f"  [删源失败] {t.id}: {e}")

    def _cleanup_stale_src(self):
        """补删（v1.2.16）：删源原本只在"任务刚下完"那一刻触发，而静默期判定属于
        "事后复查"——某番下完时还在更新（保留源），停更满 N 天后必须有人来补这一刀，
        否则源永远留在网盘上。

        每轮巡检执行：遍历本轮 scan 到的源文件（复用结果，不额外遍历 FUSE），
        本地已完整 + 闸门放行 → 删源。不依赖任务表（done 任务 1 小时后会被清掉）。
        未开任何保护策略时直接返回，保持旧的"下完即删"行为与开销。"""
        if DELETE_SRC == "false":
            return
        if DELETE_SRC != "preview" and DELETE_SRC_QUIET_DAYS <= 0 and not self.keep_entries:
            return
        for tid in sorted(self.scanned_files):
            folder, _, rel = tid.partition("/")
            if not rel:
                continue
            src = self.src_path(folder, rel)
            t = self.tasks.get(tid)
            dst = (t.dst if t is not None else None) or self.dst_path(folder, rel)  # v1.4
            s_sz = self.file_size(src)
            d_sz = self.file_size(dst)
            # 未下完 / 读不到大小 / 本地根本还没有 → 一律不动
            if s_sz <= 0 or d_sz <= 0 or s_sz != d_sz:
                continue
            keep, why = self.check_keep(folder, rel)
            if t is not None:
                t.keep = why if keep else None
            if keep:
                self._log_keep(tid, why, max_age=6 * 3600)
                continue
            if DELETE_SRC == "preview":
                log(f"  [预览-将删源·已完结] {tid}")
                continue
            try:
                os.remove(src)
                log(f"  [已删源·已完结] {tid}")
            except Exception as e:
                log(f"  [删源失败] {tid}: {e}")

    def _cleanup_src_dirs(self):
        if DELETE_SRC_DIR == "false":
            return
        for folder in FOLDERS:
            src_root = self.src_path(folder)
            if not os.path.isdir(src_root):
                continue
            for root, dirs, files in os.walk(src_root, topdown=False):
                # 只处理 src_root 下的一级子目录（避免误删 MOVIE/TV/DONGMAN 根）
                if root == src_root:
                    continue
                rel = os.path.relpath(root, src_root)
                if "/" in rel:
                    continue  # 只删一级子目录
                dst_root = self.dst_path(folder, rel)
                if not os.path.isdir(dst_root):
                    continue
                # 删源策略闸门（v1.2.16）：传 "目录/_" 让 _dir_key 取到"这个一级子目录"本身
                _keep, _why = self.check_keep(folder, rel + "/_")
                if _keep:
                    log(f"  [保留源目录·{_why}] {folder}/{rel}")
                    continue
                # 检查该目录下所有文件是否都已完整
                all_done = True
                for sroot, _, sfiles in os.walk(root):
                    for name in sfiles:
                        sf = os.path.join(sroot, name)
                        if ".rsync-partial" in sf:
                            continue
                        df = sf.replace(src_root, dst_root)
                        s_sz = self.file_size(sf)
                        d_sz = self.file_size(df)
                        # 安全校验：任一侧大小异常(<=0)视为未完成，保留目录不删
                        if d_sz <= 0 or s_sz <= 0 or s_sz != d_sz:
                            all_done = False
                            break
                    if not all_done:
                        break
                if not all_done:
                    log(f"  [保留] {folder}/{rel} 仍有文件未完全下载")
                    continue
                if DELETE_SRC_DIR == "preview":
                    log(f"  [预览-将删源目录] {folder}/{rel}")
                    continue
                try:
                    import shutil
                    shutil.rmtree(root)
                    log(f"  [已删源目录] {folder}/{rel}")
                except Exception as e:
                    log(f"  [删源目录失败] {folder}/{rel}: {e}")

    def _cleanup_empty_dirs(self):
        if DELETE_EMPTY_DIR == "false":
            return
        now = time.time()
        for folder in FOLDERS:
            src_root = self.src_path(folder)
            if not os.path.isdir(src_root):
                continue
            for root, dirs, files in os.walk(src_root, topdown=False):
                if root == src_root:
                    continue
                # 存活时长保护：太"新"的空目录不动，给上传留窗口（P1-2）
                if EMPTY_DIR_MIN_AGE > 0:
                    try:
                        mtime = os.path.getmtime(root)
                    except Exception:
                        continue  # 拿不到修改时间就保守跳过，不删
                    if now - mtime < EMPTY_DIR_MIN_AGE:
                        continue
                try:
                    rel = os.path.relpath(root, src_root).replace(os.sep, "/")
                except Exception:
                    continue
                # 受删源策略保护的目录不删（v1.2.16）：连载中的番目录留着，
                # 下一集转存进来可直接落位，不必等 QAS 重建目录
                if self.check_keep(folder, rel + "/_")[0]:
                    continue
                try:
                    os.rmdir(root)
                    log(f"  [已删空目录] {folder}/{rel}")
                except OSError:
                    pass  # 非空

    # ---------------------------- 状态保存 / HTTP ----------------------------
    def _build_status(self):
        with self.lock:
            # 文件夹聚合统计
            folder_map = {}
            for t in self.tasks.values():
                folder_map.setdefault(t.folder, {"src_total": 0, "dst_done": 0, "size_total": 0, "size_done": 0})
                folder_map[t.folder]["src_total"] += 1
                folder_map[t.folder]["size_total"] += t.src_size
                if t.state == "done":
                    folder_map[t.folder]["dst_done"] += 1
                    folder_map[t.folder]["size_done"] += t.src_size
            folders = []
            for f in FOLDERS:
                st = folder_map.get(f, {"src_total": 0, "dst_done": 0, "size_total": 0, "size_done": 0})
                prog = int(st["size_done"] * 100 / st["size_total"]) if st["size_total"] else 100
                folders.append({
                    "name": f,
                    "src_total": st["src_total"],
                    "dst_done": st["dst_done"],
                    "progress": prog,
                    "state": "done" if st["src_total"] and st["dst_done"] == st["src_total"] else "syncing"
                })
            tasks_list = [t.to_dict() for t in self.tasks.values()]
            running_n = sum(1 for t in self.tasks.values() if t.state == "running")
            pending_n = sum(1 for t in self.tasks.values() if t.state == "pending")
            done_n = sum(1 for t in self.tasks.values() if t.state == "done")
            paused_n = sum(1 for t in self.tasks.values() if t.state == "paused")
            error_n = sum(1 for t in self.tasks.values() if t.state == "error")
            return {
                "updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "state": self.global_state,
                "interval": INTERVAL,
                "concurrent": CONCURRENT,
                "running_n": running_n,
                "pending_n": pending_n,
                "done_n": done_n,
                "paused_n": paused_n,
                "error_n": error_n,
                "delete_src": DELETE_SRC,
                "delete_src_dir": DELETE_SRC_DIR,
                "delete_empty_dir": DELETE_EMPTY_DIR,
                # v1.2.16 删源保护：静默期天数 / 生效目录 / 名单条数 / 名单是否损坏
                "quiet_days": DELETE_SRC_QUIET_DAYS,
                "quiet_folders": DELETE_SRC_QUIET_FOLDERS,
                "keep_count": len(self.keep_entries),
                "keep_broken": self.keep_file_broken,
                "kept_n": sum(1 for t in self.tasks.values() if t.keep),
                "folders": folders,
                "tasks": tasks_list,
            }
    def save_status_loop(self):
        while self.running:
            self._save_status_snapshot()
            time.sleep(2)

    def _save_status_snapshot(self):
        """写一份 STATUS_FILE 快照。前端 /status 从磁盘读，所以必须把最新的
        task 状态及时落盘，前端 load() 才能看到。默认 2 秒一刷，但
        handle_control 改完状态后会主动调用一次，避免出现"server 已改、
        前端 600ms load() 还看到 paused"的卡顿错觉。"""
        try:
            status = self._build_status()
            tmp = STATUS_FILE + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(status, f, ensure_ascii=False)
            os.replace(tmp, STATUS_FILE)
        except Exception as e:
            log(f"保存状态失败: {e}")

    # ---------------------------- 断点抢救（v1.2.13） ----------------------------
    def _salvage_rsync_temp(self, t):
        """把上次硬中断遗留的 rsync 临时文件抢救为断点，避免整文件重新下载（v1.2.13）。

        rsync 中断有两种结局：① 收到 SIGTERM 优雅退出 → 半成品被挪进
        .rsync-partial/（下次自动续传，无需本方法）；② 被 SIGKILL 硬杀
        （容器被直接杀/断电/watchdog kill）→ 目标目录留下 `.<文件名>.XXXXXX`
        临时文件，rsync 重启后不认识它，只能从头下载——这就是"容器重启后任务
        重新下载、目录里多出一个新文件"的根因。

        处理：启动任务前，把同目录下该文件名匹配的最大孤儿临时文件挪进
        .rsync-partial/（rsync 认这个位置，直接从断点续传），其余更小的
        重复临时文件删除。只认严格的临时文件命名 + 60 秒内没被写过，
        不会碰正常文件和活跃下载的临时文件。"""
        base = os.path.basename(t.rel)
        dst_dir = os.path.dirname(self.dst_path(t.folder, t.rel))
        try:
            names = os.listdir(dst_dir)
        except Exception:
            return
        pat = re.compile(r"^\." + re.escape(base) + r"\.([A-Za-z0-9]{6})$")
        now = time.time()
        cands = []
        for n in names:
            if not pat.match(n):
                continue
            try:
                st = os.stat(os.path.join(dst_dir, n))
            except Exception:
                continue
            if now - st.st_mtime < 60:
                continue   # 60 秒内还在写 = 活跃下载的临时文件，别碰
            cands.append((st.st_size, n))
        if not cands:
            return
        cands.sort(reverse=True)   # (size, name) 大的在前
        partial_dir = os.path.join(self.dst_path(t.folder), ".rsync-partial")
        partial_file = os.path.join(partial_dir, base)
        try:
            cur = 0
            if os.path.exists(partial_file):
                try:
                    cur = os.path.getsize(partial_file)
                except Exception:
                    cur = 0
            best_size, best_name = cands[0]
            moved = False
            if best_size > cur:
                os.makedirs(partial_dir, exist_ok=True)
                os.replace(os.path.join(dst_dir, best_name), partial_file)
                moved = True
                log(f"[{t.id}] 断点抢救：遗留临时文件 {best_name}({fmt_size(best_size)}) 已转为断点，从 {fmt_size(best_size)} 处续传而非重新下载")
            # 其余孤儿临时文件（比断点小/旧）已无用，清理掉省空间
            for sz, n in cands:
                if moved and n == best_name:
                    continue
                try:
                    os.remove(os.path.join(dst_dir, n))
                    log(f"[{t.id}] 清理遗留临时文件 {n}({fmt_size(sz)})（已有更大的断点）")
                except Exception:
                    pass
        except Exception as e:
            log(f"[{t.id}] 断点抢救失败（不影响正常下载）: {e}")

    # ---------------------------- 优雅停止（v1.2.13） ----------------------------
    def _graceful_stop_rsyncs(self, max_wait=7.0):
        """通知所有 running 的 rsync 优雅退出（SIGTERM），并等它们把断点保存进
        .rsync-partial/ 再返回。

        容器停止/重启时 Docker 只给引擎发 SIGTERM，引擎退出后容器内剩余进程
        一律被 SIGKILL——rsync 没有机会保存断点，半成品变成孤儿临时文件，
        下次只能整文件重下。必须在引擎退出前主动给 rsync 发 SIGTERM 并等它收尾。
        注意：SIGTERM 信号处理器里会调用本方法，此时主线程可能正持有 self.lock，
        所以这里绝不能再取锁（只读任务表 + terminate 是安全的）。"""
        procs = []
        for t in list(self.tasks.values()):
            if t.state == "running" and t.proc:
                try:
                    t.proc.terminate()
                    procs.append(t.proc)
                except Exception:
                    pass
        deadline = time.time() + max_wait
        for p in procs:
            while time.time() < deadline:
                try:
                    if p.poll() is not None:
                        break
                except Exception:
                    break
                time.sleep(0.1)

    # ---------------------------- 优先级启动 / 让位（v1.2.12 / v1.2.14） ----------------------------
    def _ensure_free_slot(self, exclude_prefix=None, reason=""):
        """并发槽已满时，暂停"完成度最低"的 running 任务（done/src_size 比值最小）
        让出 1 个槽位。被让位的任务回到 pending（不丢进度，rsync --partial-dir
        断点续传），等有槽位时自动继续。

        v1.2.14 起也供文件夹级启动/重启使用：exclude_prefix 用于保证让位的
        不会是用户正要点启动的那个目录自己的任务（正常情况下该目录 running>0
        时前端根本不显示"启动"，这里是兜底）。
        仅在 handle_control 持锁期间调用：只改内存状态 + terminate（非阻塞），不做 I/O。
        返回被让位任务的 id（没有让位则返回 None）。"""
        running = [x for x in self.tasks.values() if x.state == "running"]
        if not running or len(running) < CONCURRENT:
            return None
        if exclude_prefix:
            candidates = [x for x in running if not x.id.startswith(exclude_prefix)]
            if not candidates:
                return None
        else:
            candidates = running
        def _ratio(x):
            if not x.src_size or x.src_size <= 0:
                return 0.0
            return (x.done or 0) / x.src_size
        victim = min(candidates, key=_ratio)
        if victim.proc:
            try:
                victim.proc.terminate()
            except Exception:
                pass
        victim.state = "pending"
        log(f">>> [让位] {victim.id} 完成度最低({victim.progress}%)，已暂停回队列；{reason or '为插队任务腾出槽位'}")
        return victim.id

    def _bump_priority(self, t):
        """把任务加入优先级队列【队首】，调度器会跳过普通排队任务优先启动它。

        并发槽已满时自动让位腾出 1 个槽位——否则插队任务前面还有
        CONCURRENT 个 running 在跑，网页点"启动"依旧要等很久。

        v1.2.15 关键修复：必须插到队首而非队尾。旧版 append 到队尾，
        若优先队列里已有早前的插队任务（如用户刚点过"启动文件夹"），让位
        腾出的槽位会被调度器拿去启动队首的旧插队任务，刚点"启动"的任务
        依旧开跑不了——用户实测"点了启动任务还是不跑"的深层根因。
        （handle_control 持锁期间执行，调度器 pop 队首发生在释放锁之后，
        所以先让位再插队首能保证腾出的槽一定给刚插队的任务。）"""
        self.priority.insert(0, t.id)
        self._ensure_free_slot(reason=f"{t.id} 优先启动")

    # ---------------------------- 控制指令 ----------------------------
    def handle_control(self, action, file=None, poll=False):
        with self.lock:
            if action in ("pause", "resume"):
                self.global_state = "paused" if action == "pause" else "running"
                log(f">>> 收到[{ '暂停' if action == 'pause' else '恢复' }]指令")
                if action == "pause":
                    # 暂停所有 running 任务（proc 可能缺失的边界：状态已 running 但
                    # 进程对象丢失，仍须置 paused，否则前端显示 running 却无进程）
                    for t in self.tasks.values():
                        if t.state == "running":
                            if t.proc:
                                try:
                                    t.proc.terminate()
                                except Exception:
                                    pass
                            t.state = "paused"
                # 全局指令同样立即落盘，避免前端轮询读到陈旧 state（与文件级指令一致）
                self._save_status_snapshot()
                return {"ok": True}
            if action == "restart":
                log(">>> 收到[重启]指令，优雅停止 rsync（保存断点）后引擎退出由容器自动重启")
                self.running = False
                # v1.2.13：先等 rsync 优雅退出保存断点，再退出进程。
                # 否则引擎退出后 rsync 被容器 SIGKILL 硬杀，半成品变孤儿临时文件，
                # 重启后只能整文件重下（"重启后重新下载、多出新文件"的根因之一）。
                try:
                    self._graceful_stop_rsyncs()
                except Exception:
                    pass
                return {"ok": True}
            if action == "pause_file" and file:
                if file in self.tasks:
                    t = self.tasks[file]
                    if t.state == "running" and t.proc:
                        try:
                            t.proc.terminate()
                        except Exception:
                            pass
                    t.state = "paused"
                    log(f">>> 收到[暂停文件] {file}")
                    self._save_status_snapshot()
                    return {"ok": True}
                log(f">>> [失败] pause_file 找不到任务: {repr(file)}")
                return {"ok": False, "err": "文件不存在"}
            if action == "start_file" and file:
                if file in self.tasks:
                    t = self.tasks[file]
                    if t.state in ("paused", "error"):
                        t.state = "pending"
                        t.retry = 0
                        log(f">>> 收到[启动文件] {file}")
                        self._bump_priority(t)
                    elif t.state == "pending":
                        # 已在排队：点"启动"视为"我要它先下"→ 插队（v1.2.12）
                        log(f">>> 收到[优先启动] {file}（插队；并发满时暂停完成度最低的任务让位）")
                        self._bump_priority(t)
                    else:
                        log(f">>> [忽略] start_file 状态非 pending/paused/error: state={t.state} file={file}")
                    self._save_status_snapshot()
                    return {"ok": True}
                log(f">>> [失败] start_file 找不到任务: {repr(file)}")
                return {"ok": False, "err": "文件不存在"}
            if action == "restart_file" and file:
                if file in self.tasks:
                    t = self.tasks[file]
                    if t.state == "running" and t.proc:
                        try:
                            t.proc.terminate()
                        except Exception:
                            pass
                    t.state = "pending"
                    t.retry = 0
                    t.done = 0
                    t.progress = 0
                    t.speed = "-"
                    t.eta = "-"
                    log(f">>> 收到[重启文件] {file}")
                    self._bump_priority(t)   # 重启同样插队（v1.2.12）
                    self._save_status_snapshot()
                    return {"ok": True}
                log(f">>> [失败] restart_file 找不到任务: {repr(file)}")
                return {"ok": False, "err": "文件不存在"}
            # 文件夹级批量控制：对指定前缀下的所有任务生效
            if action == "pause_folder" and file:
                prefix = file.rstrip("/") + "/"
                n = 0
                for tid, t in self.tasks.items():
                    if tid.startswith(prefix) or tid == file:
                        if t.state == "running" and t.proc:
                            try:
                                t.proc.terminate()
                            except Exception:
                                pass
                        if t.state in ("running", "pending"):
                            t.state = "paused"
                            n += 1
                log(f">>> 收到[暂停文件夹] {file} ({n} 个任务)")
                self._save_status_snapshot()
                return {"ok": True, "n": n}
            if action == "start_folder" and file:
                prefix = file.rstrip("/") + "/"
                n = 0
                new_ids = []
                for tid, t in self.tasks.items():
                    # v1.2.14：pending 也纳入插队。旧版只处理 paused/error——
                    # 槽位被其他目录占满时，目录里全是 pending 的任务点"启动"
                    # 既不入优先级也不让位，等于毫无效果（用户实测问题根因）
                    if (tid.startswith(prefix) or tid == file) and t.state in ("paused", "error", "pending"):
                        if t.state in ("paused", "error"):
                            t.state = "pending"
                            t.retry = 0
                        new_ids.append(tid)
                        n += 1
                # v1.2.15：整体插到优先队列【最前】。旧版 append 队尾会被
                # 更早入队的插队任务（如之前点过的其他文件夹）抢走腾出的槽位
                self.priority[:0] = new_ids
                # 插队后保证立刻有槽位可用：并发满时让其他目录完成度最低的任务让位
                self._ensure_free_slot(exclude_prefix=prefix, reason=f"启动文件夹 {file}")
                log(f">>> 收到[启动文件夹] {file} ({n} 个任务插队)")
                self._save_status_snapshot()
                return {"ok": True, "n": n}
            if action == "restart_folder" and file:
                prefix = file.rstrip("/") + "/"
                n = 0
                new_ids = []
                for tid, t in self.tasks.items():
                    if tid.startswith(prefix) or tid == file:
                        if t.state == "running" and t.proc:
                            try:
                                t.proc.terminate()
                            except Exception:
                                pass
                        if t.state != "pending":
                            t.state = "pending"
                            t.retry = 0
                            t.done = 0
                            t.progress = 0
                            t.speed = "-"
                            t.eta = "-"
                            n += 1
                        new_ids.append(tid)
                # v1.2.15：插到优先队列【最前】（同 start_folder）
                self.priority[:0] = new_ids
                # v1.2.14：目录自身没有 running 任务时槽位可能仍被其他目录占满，
                # 同样让位，保证点完"重启"立刻开跑
                self._ensure_free_slot(exclude_prefix=prefix, reason=f"重启文件夹 {file}")
                log(f">>> 收到[重启文件夹] {file} ({n} 个任务)")
                self._save_status_snapshot()
                return {"ok": True, "n": n}
            if action == "remove_file" and file:
                # 手动移除任务记录（v1.2.12）：只清引擎里的任务条目和网页列表，
                # 不删本地文件、不动网盘。若网盘源文件还在，下轮 scan() 会重新入队。
                if file in self.tasks:
                    t = self.tasks[file]
                    if t.state == "running" and t.proc:
                        try:
                            t.proc.terminate()
                        except Exception:
                            pass
                    del self.tasks[file]
                    log(f">>> 收到[移除任务] {file}（若网盘源还在，下轮巡检会重新入队）")
                    self._save_status_snapshot()
                    return {"ok": True}
                log(f">>> [失败] remove_file 找不到任务: {repr(file)}")
                return {"ok": False, "err": "文件不存在"}
            if action == "clear_errors":
                n = 0
                for tid in list(self.tasks.keys()):
                    if self.tasks[tid].state == "error":
                        del self.tasks[tid]
                        n += 1
                log(f">>> 收到[清除失败] 共移除 {n} 个失败任务")
                self._save_status_snapshot()
                return {"ok": True, "n": n}
            if action == "kick":
                # v1.2.17 紧链路：quark-pipeline 编排建完任务后触发"立即巡检"。
                # 只置标志，由 main_loop 的休眠轮询消费（正在巡检时也安全）。
                # v1.5 poll=1：转存刚完成时 rclone/FUSE 目录可见性有 1-2 分钟延迟，
                # 立即巡检常扫到 0/0。poll 模式下空扫就 30 秒后重扫，最多 6 次（3 分钟）。
                self._kick = True
                self._kick_poll = 6 if poll else 0
                log(">>> 收到[立即巡检]指令%s" % ("（轮询模式，容忍挂载缓存延迟）" if poll else ""))
                return {"ok": True}
            log(f">>> [失败] 未知指令: {action}")
            return {"ok": False, "err": "未知指令"}

    # ---------------------------- 主循环 ----------------------------
    def main_loop(self):
        while self.running:
            if self.global_state == "paused":
                time.sleep(1)
                continue
            log(f"===== 开始巡检（并发 {CONCURRENT}）=====")
            # v1.2.11:不再每轮主动刷新挂载缓存——AList WebDAV 自带 ETag/TTL 刷新，scan 用 RSS 风格的 stat 即可
            #         之前这里走 refresh_remote_cache() 给 rclone/alist 发 SIGHUP，会刷"未找到目标进程"日志
            self.scan()
            # v1.5 kick 轮询模式：转存刚完成时 FUSE 挂载的目录可见性有 1-2 分钟延迟，
            # 立即巡检往往 0/0。poll 模式下空手而归就等 30 秒重扫（最多 6 次），
            # 一旦发现任务或已有任务在跑就退出轮询，恢复正常节奏。
            if self._kick_poll > 0:
                with self.lock:
                    busy = any(t.state in ("pending", "running")
                               for t in self.tasks.values())
                if busy:
                    self._kick_poll = 0
                    log(">>> [kick轮询] 已发现任务，轮询结束")
                else:
                    self._kick_poll -= 1
                    log(">>> [kick轮询] 未发现新文件（挂载缓存延迟？），30 秒后重扫（剩 %d 次）"
                        % self._kick_poll)
                    time.sleep(30)
                    continue
            # 等待本轮任务基本跑完（最多等 INTERVAL 时间）
            waited = 0
            while waited < INTERVAL and self.running and self.global_state != "paused":
                with self.lock:
                    pending = [t for t in self.tasks.values() if t.state == "pending"]
                    running = [t for t in self.tasks.values() if t.state == "running"]
                    if not pending and not running:
                        break
                time.sleep(2)
                waited += 2
            # 续传兜底：对 error 任务重试
            with self.lock:
                for t in self.tasks.values():
                    if t.state == "error" and t.retry < MAX_RETRY:
                        t.retry += 1
                        t.state = "pending"
                        log(f"[{t.id}] 自动重试 {t.retry}/{MAX_RETRY}")
            # 再跑一阵
            waited2 = 0
            while waited2 < 30 and self.running and self.global_state != "paused":
                with self.lock:
                    running = [t for t in self.tasks.values() if t.state == "running"]
                    pending = [t for t in self.tasks.values() if t.state == "pending"]
                    if not pending and not running:
                        break
                time.sleep(2)
                waited2 += 2
            # 清理
            self._cleanup_stale_src()   # v1.2.16：事后补删（停更满静默期的番，届时才放行删源）
            self._cleanup_src_dirs()
            self._cleanup_empty_dirs()
            self._save_activity()       # 持久化目录活跃度：容器重建不该让静默期从头计时
            # 统计
            with self.lock:
                done_count = sum(1 for t in self.tasks.values() if t.state == "done")
                total_count = len(self.tasks)
                new_count = sum(1 for t in self.tasks.values() if t.state == "done" and t.last_update == 0)
                # 清理长期 done 的任务（保留 1 小时）
                now = time.time()
                done_tasks = {k: t for k, t in self.tasks.items() if t.state == "done" and now - t.last_update > 3600}
                for k in done_tasks:
                    del self.tasks[k]
            log(f"===== 巡检结束：本轮完成 {done_count}/{total_count}，休眠 {INTERVAL} 秒 =====")
            # 长间隔拆成短轮询，方便响应暂停/重启
            for _ in range(INTERVAL // 2):
                if not self.running or self.global_state == "paused":
                    break
                self._check_control_file()
                # v1.2.17 kick：外部编排（quark-pipeline）刚建完转存任务时写 control="kick"，
                # 立即结束休眠重新巡检——新资源最多 2 秒内开始下载，不再干等 10 分钟轮询。
                # 若 kick 在巡检进行中写入，这里会在巡检结束后的第一次短轮询读到，效果一致。
                if getattr(self, "_kick", False):
                    self._kick = False
                    log(">>> [立即巡检] 提前结束休眠")
                    break
                time.sleep(2)

    def _check_control_file(self):
        if not os.path.exists(CONTROL_FILE):
            return
        try:
            with open(CONTROL_FILE, "r", encoding="utf-8") as f:
                data = f.read().strip()
            os.remove(CONTROL_FILE)
            if not data:
                return
            parts = data.split("|", 1)
            action = parts[0]
            file_arg = parts[1] if len(parts) > 1 else None
            self.handle_control(action, file_arg)
        except Exception as e:
            log(f"读取控制文件失败: {e}")

# -----------------------------------------------------------------------------
# HTTP 服务
# -----------------------------------------------------------------------------

def make_html():
    return r"""<!DOCTYPE html>
<html lang="zh-CN"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>夸克同步监控</title>
<style>
:root{--bg:#0b0d12;--card:#151821;--fg:#e8eaed;--muted:#8b93a1;--acc:#3b82f6;--ok:#22c55e;--warn:#f59e0b;--red:#ef4444;--bor:#2a2f3a;}
*{box-sizing:border-box}
body{margin:0;background:var(--bg);color:var(--fg);font-family:-apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif;padding:16px}
h1{font-size:20px;margin:0 0 4px}
.sub{color:var(--muted);font-size:13px;margin-bottom:16px}
.card{background:var(--card);border:1px solid var(--bor);border-radius:12px;padding:14px;margin-bottom:12px}
.ov{display:flex;gap:16px;flex-wrap:wrap;font-size:13px}
.ov b{font-size:16px}
.ov .c-run{color:var(--acc)} .ov .c-pend{color:var(--warn)} .ov .c-done{color:var(--ok)} .ov .c-err{color:var(--red)} .ov .c-keep{color:#60a5fa}
.state{display:inline-block;padding:4px 12px;border-radius:20px;font-size:12px;font-weight:700}
.running{background:rgba(34,197,94,.15);color:var(--ok)}
.paused{background:rgba(245,158,11,.15);color:var(--warn)}
/* v1.2.16 删源保护徽章：标出「源正在被保护、不会删」的番剧目录与文件 */
.keepbadge{display:inline-block;margin-left:6px;padding:1px 7px;border-radius:10px;font-size:11px;font-weight:600;background:rgba(59,130,246,.18);color:#60a5fa;border:1px solid rgba(59,130,246,.35)}
.tree{font-size:13px}
.node{position:relative}
.node-head{display:flex;align-items:center;gap:10px;padding:8px 6px;border-bottom:1px solid var(--bor);cursor:pointer;transition:background .15s}
.node-head:hover{background:rgba(255,255,255,.03)}
.node-head .arrow{width:14px;height:14px;display:inline-flex;align-items:center;justify-content:center;color:var(--muted);transition:transform .2s}
.node-head.expanded .arrow{transform:rotate(90deg)}
.node-head.leaf .arrow{visibility:hidden}
.node-head .name{flex:1;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.node-head .name .path{color:var(--muted);font-size:11px;margin-left:6px}
.node-head .barwrap{width:120px;min-width:120px}
.node-head .bar{height:8px;background:#252a33;border-radius:4px;overflow:hidden}
.node-head .fill{height:100%;background:linear-gradient(90deg,var(--acc),#60a5fa);box-shadow:0 0 8px rgba(59,130,246,.5);transition:width .4s}
.node-head .meta{width:170px;min-width:170px;color:var(--muted);font-size:11px;text-align:right;white-space:nowrap}
.node-head .btns{display:flex;gap:6px;min-width:150px;justify-content:flex-end}
.btn{position:relative;overflow:hidden;display:inline-flex;align-items:center;justify-content:center;padding:5px 11px;border:0;border-radius:7px;font-size:11px;font-weight:700;letter-spacing:.3px;color:#fff;cursor:pointer;-webkit-tap-highlight-color:transparent;transition:transform .14s cubic-bezier(.2,.8,.2,1),box-shadow .22s ease,filter .2s ease;box-shadow:0 1px 2px rgba(0,0,0,.35)}
.pause{background:linear-gradient(135deg,#fbbf24,#f59e0b);box-shadow:0 2px 10px rgba(245,158,11,.38)}
.start{background:linear-gradient(135deg,#4ade80,#16a34a);box-shadow:0 2px 10px rgba(34,197,94,.38)}
.restart{background:linear-gradient(135deg,#f87171,#dc2626);box-shadow:0 2px 10px rgba(239,68,68,.38)}
.remove{background:linear-gradient(135deg,#64748b,#475569);box-shadow:0 2px 8px rgba(71,85,105,.35);padding:5px 8px}
.btn:hover{transform:translateY(-2px);filter:brightness(1.08);box-shadow:0 6px 18px rgba(0,0,0,.45)}
.btn:active{transform:translateY(0) scale(.94);filter:brightness(.92);box-shadow:0 1px 3px rgba(0,0,0,.4)}
.btn:focus-visible{outline:2px solid rgba(255,255,255,.55);outline-offset:2px}
.btn.busy{opacity:.55;pointer-events:none}
.btn.busy::after{content:"";position:absolute;right:8px;top:50%;width:10px;height:10px;margin-top:-5px;border:2px solid rgba(255,255,255,.35);border-top-color:#fff;border-radius:50%;animation:spin .7s linear infinite}
/* v1.2.14 操作结果闪烁：请求成功绿色光圈、失败红色光圈，1 秒内一眼可见 */
.btn.ok{box-shadow:0 0 0 3px rgba(34,197,94,.5),0 4px 18px rgba(34,197,94,.45)!important;transform:scale(1.07)}
.btn.err{box-shadow:0 0 0 3px rgba(239,68,68,.55)!important;transform:scale(1.07)}
.btn .ripple{position:absolute;border-radius:50%;transform:scale(0);background:rgba(255,255,255,.5);animation:ripple .6s ease-out;pointer-events:none}
@keyframes ripple{to{transform:scale(2.8);opacity:0}}
@keyframes spin{to{transform:rotate(360deg)}}
.badge{display:inline-block;padding:1px 7px;border-radius:10px;font-size:11px;font-weight:600;margin-left:6px}
.b-running{background:rgba(34,197,94,.15);color:var(--ok)}
.b-pending{background:rgba(139,147,161,.15);color:var(--muted)}
.b-paused{background:rgba(245,158,11,.15);color:var(--warn)}
.b-done{background:rgba(34,197,94,.15);color:var(--ok)}
.b-error{background:rgba(239,68,68,.15);color:var(--red)}
.children{padding-left:18px;border-left:1px dashed var(--bor);margin-left:7px;display:none}
.children.expanded{display:block}
/* v1.2.14 展开过渡动画：只在用户手动展开（toggle/expandAll/expandDepth 追加 .anim）时
   播放一次。轮询重渲染重建 DOM 时不带 .anim，避免已展开的目录每 3 秒闪一次。 */
.children.expanded.anim{animation:treeExpand .25s ease}
@keyframes treeExpand{from{opacity:0;transform:translateY(-6px)}to{opacity:1;transform:translateY(0)}}
/* v1.2.14 操作结果 toast（参考 Toastify 的右上角滑入模式） */
#toasts{position:fixed;top:14px;right:14px;z-index:99;display:flex;flex-direction:column;gap:8px;max-width:340px;pointer-events:none}
.toast{background:var(--card);border:1px solid var(--bor);border-left:3px solid var(--acc);border-radius:9px;padding:9px 14px;font-size:12.5px;color:var(--fg);box-shadow:0 8px 28px rgba(0,0,0,.45);animation:toastIn .28s cubic-bezier(.2,.8,.2,1)}
.toast.ok{border-left-color:var(--ok)}
.toast.warn{border-left-color:var(--warn)}
.toast.err{border-left-color:var(--red)}
.toast.out{opacity:0;transform:translateX(18px);transition:all .25s ease}
@keyframes toastIn{from{opacity:0;transform:translateX(30px)}to{opacity:1;transform:translateX(0)}}
.file-row .arrow{visibility:hidden}
.file-row .name{font-weight:400}
.folder-row .name{font-weight:600}
.tools{display:flex;gap:10px;align-items:center;margin-bottom:10px}
.tools button{position:relative;overflow:hidden;padding:6px 12px;border:1px solid var(--bor);border-radius:7px;background:var(--card);color:var(--fg);font-size:12px;font-weight:600;cursor:pointer;transition:transform .14s cubic-bezier(.2,.8,.2,1),background .2s,box-shadow .2s,border-color .2s}
.tools button:hover{background:rgba(255,255,255,.07);border-color:var(--acc);box-shadow:0 4px 14px rgba(59,130,246,.25);transform:translateY(-1px)}
.tools button:active{transform:translateY(0) scale(.96)}
.empty{color:var(--muted);font-size:13px;padding:10px 0}
.ctrl{display:flex;gap:10px;margin-top:6px}
.ctrl button{position:relative;overflow:hidden;flex:1;padding:11px 10px;border:0;border-radius:9px;font-size:14px;font-weight:700;letter-spacing:.3px;color:#fff;cursor:pointer;transition:transform .14s cubic-bezier(.2,.8,.2,1),box-shadow .22s ease,filter .2s ease}
.ctrl button:hover{transform:translateY(-2px);filter:brightness(1.08);box-shadow:0 8px 22px rgba(0,0,0,.45)}
.ctrl button:active{transform:translateY(0) scale(.97);filter:brightness(.9)}
.ctrl button:focus-visible{outline:2px solid rgba(255,255,255,.55);outline-offset:2px}
.tip{color:var(--muted);font-size:12px;margin-top:8px}
</style></head>
<body>
<h1>夸克网盘 → fnOS 同步监控</h1>
<div id="toasts"></div>
<div class="sub" id="updated">加载中…</div>
<div class="card">
  <div>状态：<span id="state" class="state running">-</span></div>
  <div class="meta" id="cfg"></div>
</div>
<div class="card">
  <div style="font-weight:600;margin-bottom:8px">下载概览（并发上限 <span id="conc">-</span>）</div>
  <div class="ov" id="overview"></div>
</div>
<div class="card">
  <div class="tools">
    <span style="font-weight:600">同步目录树</span>
    <button onclick="expandAll()">展开全部</button>
    <button onclick="collapseAllHard()">折叠全部</button>
    <button onclick="expandDepth(1)">展开1层</button>
    <button onclick="resetTreeState()">恢复默认</button>
    <button id="clearErrBtn" style="display:none" onclick="ctrl('clear_errors')">✕ 清除失败</button>
  </div>
  <div id="tree" class="tree"></div>
  <div id="empty" class="empty" style="display:none">暂无任务</div>
</div>
<div class="card">
  <div class="ctrl">
    <button class="pause" onclick="ctrl('pause')">⏸ 全部暂停</button>
    <button class="start" onclick="ctrl('resume')">▶ 全部恢复</button>
    <button class="restart" onclick="ctrl('restart')">⟳ 重启同步</button>
  </div>
  <div class="tip">暂停=停止下载（容器不退出）；恢复=继续；重启=重建容器并立即跑一轮。点文件或目录的"启动/重启"会插队优先下载：并发槽满时自动暂停其他目录里完成度最低的任务让位（进度保留、自动续传），点完目录立刻开跑。操作结果见右上角提示。文件行的 ✕ 仅移除列表记录，不动本地文件</div>
</div>
<script>
var expandedSet=new Set();
/* 用户手动操作过的节点（点箭头 / 展开全部 / 折叠全部 / 展开N层）。
   一旦用户表态，后续轮询重渲染就完全尊重用户意图，不再按默认层级强制展开。 */
var userTouched=new Set();
/* 已渲染过的节点，用于区分"新出现的文件夹"（可套用默认展开层级）
   和"老节点"（沿用用户/现有展开状态） */
var seenKeys=new Set();
/* 新出现的文件夹默认展开到第几层。点「折叠全部」后设为 0（新节点也不再自动弹开），
   「展开全部」设为 999，「恢复默认」设回 1 */
var autoExpandDepth=1;
/* v1.2.14 toast：操作结果右上角滑入，2.6 秒后滑出。重渲染不会把它冲掉，
   是"点了到底有没有生效"的主要视觉凭证 */
function toast(msg,kind){var box=document.getElementById('toasts');if(!box)return;var d=document.createElement('div');d.className='toast '+(kind||'');d.textContent=msg;box.appendChild(d);while(box.children.length>4)box.removeChild(box.firstChild);setTimeout(function(){d.classList.add('out');setTimeout(function(){d.remove();},260);},2600);}
var ACT_NAME={pause:'全局暂停',resume:'全局恢复',restart:'重启同步',pause_file:'暂停文件',start_file:'启动文件',restart_file:'重启文件',pause_folder:'暂停目录',start_folder:'启动目录',restart_folder:'重启目录',remove_file:'移除记录',clear_errors:'清除失败',kick:'立即巡检'};
function ctrl(a,f,btn){
  var name=ACT_NAME[a]||a;
  var tgt=f?f.split('/').slice(-2).join('/'):'';
  if(btn)btn.classList.add('busy');
  var url='/control?action='+a;if(f)url+='&file='+encodeURIComponent(f);
  fetch(url,{method:'POST'}).then(r=>r.json()).then(d=>{
    var mark=d.ok?'ok':'err';
    if(btn){btn.classList.remove('busy');btn.classList.add(mark);setTimeout(function(){if(btn)btn.classList.remove(mark);},900);}
    if(d.ok){
      var tail=(typeof d.n==='number')?(a==='clear_errors'?'（'+d.n+' 条）':'（'+d.n+' 个任务）'):'';
      var kind=(a==='pause'||a==='pause_file'||a==='pause_folder'||a==='remove_file')?'warn':'ok';
      toast(name+(tgt?'：'+tgt:'')+tail,kind);
    }else{
      toast(name+' 失败：'+(d.err||'未知错误'),'err');
    }
    setTimeout(load,600);
  }).catch(function(){
    if(btn){btn.classList.remove('busy');btn.classList.add('err');setTimeout(function(){if(btn)btn.classList.remove('err');},900);}
    toast('请求失败：服务无响应','err');
  });
}
function stBadge(s){
  var m={running:['下载中','b-running'],pending:['等待','b-pending'],paused:['已暂停','b-paused'],done:['完成','b-done'],error:['失败','b-error']};
  var x=m[s];if(!x)return '';
  return '<span class="badge '+x[1]+'">'+x[0]+'</span>';
}
function fmtSize(n){if(n<1024)return n+'B';if(n<1048576)return (n/1024).toFixed(2)+'K';if(n<1073741824)return (n/1048576).toFixed(2)+'M';return (n/1073741824).toFixed(2)+'G';}
function fmtSecs(s){if(!s||s<0)return '-';var h=Math.floor(s/3600),m=Math.floor((s%3600)/60),ss=Math.floor(s%60);if(h)return h+'时'+m+'分';return m+'分'+ss+'秒';}
function pathParts(id){return id.replace(/\\/g,'/').split('/').filter(Boolean);}
function buildTree(tasks){
  var root={children:{},files:[]};
  tasks.forEach(t=>{
    var parts=pathParts(t.id);
    if(parts.length<2){root.files.push(t);return;}
    var node=root;
    for(var i=0;i<parts.length-1;i++){
      var name=parts[i];
      if(!node.children[name])node.children[name]={name:name,children:{},files:[],depth:i+1};
      node=node.children[name];
    }
    node.files.push(t);
  });
  return root;
}
function aggNode(node){
  var size=0,done=0,running=0,pending=0,paused=0,error=0,doneCount=0,total=0,speedSum=0,etaSum=0,etaCount=0,kept=0;
  function walk(n){
    n.files.forEach(t=>{
      total++;size+=t.src_size||0;
      if(t.keep)kept++;   /* v1.2.16：受删源保护（连载中/名单命中）的文件数 */
      if(t.state==='done'){done+=t.src_size||0;doneCount++;}
      else if(t.state==='running'){running++;done+=t.done||0;if(t.speed){speedSum+=parseSpeed(t.speed);}if(t.eta&&t.eta!=='计算中'&&t.eta!=='-'){etaSum+=parseEta(t.eta);etaCount++;}}
      else if(t.state==='pending')pending++;
      else if(t.state==='paused')paused++;
      else if(t.state==='error')error++;
    });
    for(var k in n.children)walk(n.children[k]);
  }
  walk(node);
  var progress=size>0?Math.round(done*100/size):0;
  var speedStr=speedSum>0?fmtSpeed(speedSum):'-';
  var etaStr=etaCount>0?fmtSecs(Math.round(etaSum/etaCount)):'-';
  return {size,done,progress,total,doneCount,running,pending,paused,error,speedStr,etaStr,kept};
}
function parseSpeed(s){if(!s||s==='-')return 0;var m=s.replace(/,/g,'').match(/^([\d.]+)\s*([BKMGTP])B\/s$/i);if(!m)return 0;var u={B:1,K:1024,M:1048576,G:1073741824,T:1099511627776};return parseFloat(m[1])*(u[m[2].toUpperCase()]||1);}
function fmtSpeed(b){if(b<1024)return b.toFixed(0)+'B/s';if(b<1048576)return (b/1024).toFixed(2)+'KB/s';if(b<1073741824)return (b/1048576).toFixed(2)+'MB/s';return (b/1073741824).toFixed(2)+'GB/s';}
function parseEta(s){if(!s)return 0;var parts=s.match(/(\d+)时(\d+)分/);if(parts)return parseInt(parts[1])*3600+parseInt(parts[2])*60;parts=s.match(/(\d+)分(\d+)秒/);if(parts)return parseInt(parts[1])*60+parseInt(parts[2]);return 0;}
function esc(s){return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;').replace(/'/g,'&#39;');}
/* 给按钮直接绑定 click（元素级，不依赖事件冒泡）；dataset.bound 防重复绑定。
   与底部的 document 事件委托互为兜底：stopPropagation 保证只触发一次。 */
function bindBtns(scope){
  if(!scope||!scope.querySelectorAll)return;
  var list=scope.querySelectorAll('.btn[data-action]');
  for(var i=0;i<list.length;i++){
    var b=list[i];
    if(b.dataset.bound)continue;
    b.dataset.bound='1';
    b.addEventListener('click',function(ev){
      spawnRipple(this,ev);
      ev.preventDefault();ev.stopPropagation();
      ctrl(this.getAttribute('data-action'),this.getAttribute('data-file'),this);
    });
  }
}
function folderBtns(path,stats){
  var e=esc(path);
  var b='';
  /* v1.2.13：目录里有任务正在下载才显示「暂停」；没有在下载（只有排队/暂停/失败）
     则显示「启动」（插队优先下载/恢复），与文件级按钮行为一致；全部完成的目录只剩「重启」。 */
  if(stats.running>0)b+='<button class="btn pause" data-action="pause_folder" data-file="'+e+'">⏸ 暂停</button>';
  else if(stats.pending>0||stats.paused>0||stats.error>0)b+='<button class="btn start" data-action="start_folder" data-file="'+e+'">▶ 启动</button>';
  b+='<button class="btn restart" data-action="restart_folder" data-file="'+e+'">⟳ 重启</button>';
  return b;
}
function fileBtns(t){
  var e=esc(t.id);
  if(t.state==='running')return '<button class="btn pause" data-action="pause_file" data-file="'+e+'">⏸ 暂停</button><button class="btn restart" data-action="restart_file" data-file="'+e+'">⟳ 重启</button>';
  var b='<button class="btn start" data-action="start_file" data-file="'+e+'">▶ 启动</button><button class="btn restart" data-action="restart_file" data-file="'+e+'">⟳ 重启</button>';
  /* ✕ 仅移除这条任务记录（v1.2.12）：不删本地文件；网盘源若还在，下轮巡检会重新入队 */
  if(t.state==='error'||t.state==='paused'||t.state==='done')b+='<button class="btn remove" data-action="remove_file" data-file="'+e+'" title="仅从列表移除这条记录，不影响本地文件；若网盘源文件还在，下轮巡检会重新出现">✕</button>';
  return b;
}
function renderNode(node,path,container){
  var isRoot=!path;
  var stats=aggNode(node);
  if(isRoot){
    if(stats.total===0){container.innerHTML='<div class="empty">暂无任务</div>';return;}
    for(var name in node.children){
      var childPath=name;
      renderNode(node.children[name],childPath,container);
    }
    node.files.forEach(t=>renderFile(t,container));
    return;
  }
  var idKey=path;
  /* 展开状态的优先级：用户手动操作 > 新节点默认层级 > 已有状态。
     旧版这里是无条件的 `|| node.depth===1`，导致每轮轮询重渲染都把第1层强行展开，
     用户手动折叠和"折叠全部"3秒后即失效。 */
  var isNew=!seenKeys.has(idKey);
  seenKeys.add(idKey);
  var expanded;
  if(userTouched.has(idKey)){
    expanded=expandedSet.has(idKey);
  }else if(isNew){
    expanded=(node.depth<=autoExpandDepth);   // 新节点套用当前默认展开层级
    if(expanded)expandedSet.add(idKey);       // 固化下来，后续沿用
  }else{
    expanded=expandedSet.has(idKey);
  }
  var div=document.createElement('div');div.className='node';
  var head=document.createElement('div');head.className='node-head folder-row'+(expanded?' expanded':'');
  head.dataset.path=idKey;
  head.onclick=function(){toggle(idKey);};
  head.innerHTML='<span class="arrow">▶</span><span class="name">'+esc(node.name)+'<span class="path">'+stats.doneCount+'/'+stats.total+' ｜ '+fmtSize(stats.size)+'</span>'+stBadge(stats.running>0?'running':(stats.paused>0?'paused':(stats.pending>0?'pending':(stats.error>0?'error':'done'))))+(stats.kept?'<span class="keepbadge" title="该目录下有源受保护（连载中 / 名单命中），不会被删源">🔒'+stats.kept+'</span>':'')+'</span>'+
    '<div class="barwrap"><div class="bar"><div class="fill" style="width:'+stats.progress+'%"></div></div></div>'+
    '<div class="meta">'+stats.progress+'% ｜ '+esc(stats.speedStr)+' ｜ '+esc(stats.etaStr)+'</div>'+
    '<div class="btns">'+folderBtns(idKey,stats)+'</div>';
  bindBtns(head);
  div.appendChild(head);
  var children=document.createElement('div');children.className='children'+(expanded?' expanded':'');children.dataset.parent=idKey;
  for(var name in node.children){
    var childPath=path+'/'+name;
    renderNode(node.children[name],childPath,children);
  }
  node.files.forEach(t=>renderFile(t,children,path));
  div.appendChild(children);
  container.appendChild(div);
}
function renderFile(t,container,parentPath){
  var div=document.createElement('div');div.className='node file-row';
  var parts=pathParts(t.id);
  var fname=parts[parts.length-1]||t.id;
  var head=document.createElement('div');head.className='node-head leaf';
  head.innerHTML='<span class="arrow">▶</span><span class="name">'+esc(fname)+stBadge(t.state)+(t.keep?'<span class="keepbadge" title="'+esc(t.keep)+'">🔒 源保留</span>':'')+'</span>'+
    '<div class="barwrap"><div class="bar"><div class="fill" style="width:'+t.progress+'%"></div></div></div>'+
    '<div class="meta">'+t.progress+'% ｜ '+esc(t.speed)+' ｜ '+esc(t.eta)+'</div>'+
    '<div class="btns">'+fileBtns(t)+'</div>';
  bindBtns(head);
  div.appendChild(head);
  container.appendChild(div);
}
function toggle(path){
  var head=document.querySelector('.node-head[data-path="'+CSS.escape(path)+'"]');
  var child=document.querySelector('.children[data-parent="'+CSS.escape(path)+'"]');
  if(!head||!child)return;
  userTouched.add(path);   // 用户手动表态，后续轮询不再覆盖
  if(head.classList.contains('expanded')){head.classList.remove('expanded');child.classList.remove('expanded','anim');expandedSet.delete(path);}
  else{head.classList.add('expanded');child.classList.add('expanded','anim');expandedSet.add(path);}  // v1.2.14：.anim 让本次展开播放过渡动画
}
function expandAll(){autoExpandDepth=999;document.querySelectorAll('.node-head.folder-row').forEach(h=>{userTouched.add(h.dataset.path);var c=document.querySelector('.children[data-parent="'+CSS.escape(h.dataset.path)+'"]');if(!h.classList.contains('expanded')&&c){c.classList.add('anim');}if(!h.classList.contains('expanded')){h.classList.add('expanded');if(c)c.classList.add('expanded');expandedSet.add(h.dataset.path);}});}
function collapseAll(){document.querySelectorAll('.node-head.folder-row').forEach(h=>{userTouched.add(h.dataset.path);if(h.classList.contains('expanded')){h.classList.remove('expanded');var c=document.querySelector('.children[data-parent="'+CSS.escape(h.dataset.path)+'"]');if(c)c.classList.remove('expanded');expandedSet.delete(h.dataset.path);}});}
/* 折叠全部（含后续新出现的节点）：清空展开状态、把默认展开层级降为 0，
   并给现存节点打上 touched 标记，三方合力确保之后不会被轮询重新弹开 */
function collapseAllHard(){
  autoExpandDepth=0;
  expandedSet.clear();
  document.querySelectorAll('.node-head.folder-row').forEach(h=>{
    userTouched.add(h.dataset.path);
    h.classList.remove('expanded');
    var c=document.querySelector('.children[data-parent="'+CSS.escape(h.dataset.path)+'"]');
    if(c)c.classList.remove('expanded');
  });
}
/* 恢复默认：清掉所有手动标记，按默认层级（第1层展开）重新渲染 */
function resetTreeState(){
  userTouched.clear();seenKeys.clear();expandedSet.clear();autoExpandDepth=1;
  renderTree(lastTasks||[]);
}
function expandDepth(maxDepth){
  autoExpandDepth=maxDepth;   // 同时作为后续新节点的默认层级
  document.querySelectorAll('.node').forEach(n=>{
    var h=n.querySelector(':scope > .node-head.folder-row');
    var c=n.querySelector(':scope > .children');
    if(!h||!c)return;
    userTouched.add(h.dataset.path);
    var depth=(h.dataset.path.match(/\//g)||[]).length+1;
    if(depth<=maxDepth){if(!h.classList.contains('expanded')&&c)c.classList.add('anim');h.classList.add('expanded');c.classList.add('expanded');expandedSet.add(h.dataset.path);}
    else{h.classList.remove('expanded');c.classList.remove('expanded','anim');expandedSet.delete(h.dataset.path);}
  });
}
var lastTasks=null;   // 最近一次的任务快照，供「恢复默认」本地重渲染复用
function renderTree(tasks){
  lastTasks=tasks;
  var container=document.getElementById('tree');
  container.innerHTML='';
  if(!tasks.length){document.getElementById('empty').style.display='block';return;}
  document.getElementById('empty').style.display='none';
  var tree=buildTree(tasks);
  renderNode(tree,'',container);
}
function load(){
  fetch('/status').then(r=>r.json()).then(s=>{
    document.getElementById('updated').textContent='更新于 '+s.updated+' ｜ 间隔 '+s.interval+'s ｜ 并发上限 '+s.concurrent;
    document.getElementById('conc').textContent=s.concurrent;
    var ov='正在下载 <b class="c-run">'+s.running_n+'</b> ｜ 排队 <b class="c-pend">'+s.pending_n+'</b> ｜ 完成 <b class="c-done">'+s.done_n+'</b> ｜ 暂停 <b>'+s.paused_n+'</b> ｜ 失败 <b class="c-err">'+s.error_n+'</b>'+((s.kept_n>0)?' ｜ 保护源 <b class="c-keep">'+s.kept_n+'</b>':'');
    document.getElementById('overview').innerHTML=ov;
    var ce=document.getElementById('clearErrBtn');if(ce)ce.style.display=(s.error_n>0)?'':'none';
    var st=document.getElementById('state');
    st.textContent=s.state==='paused'?'已暂停':'运行中';
    st.className='state '+(s.state==='paused'?'paused':'running');
    /* v1.2.16：删源保护配置一眼可见——连载中的番会一直挂着「🔒保护源」不删 */
    var qs='';
    if(s.quiet_days>0)qs+=' ｜ 静默期:'+s.quiet_days+'天'+((s.quiet_folders&&s.quiet_folders.length)?('['+s.quiet_folders.join('/')+']'):'');
    if(s.keep_count>0)qs+=' ｜ 保护名单:'+s.keep_count+'条';
    if(s.keep_broken)qs+=' ｜ ⚠️名单损坏·已暂停删源';
    document.getElementById('cfg').textContent='删源:'+s.delete_src+' ｜ 删源目录:'+s.delete_src_dir+' ｜ 删空目录:'+s.delete_empty_dir+qs;
    renderTree(s.tasks||[]);
  }).catch(e=>{document.getElementById('updated').textContent='连接失败：'+e;});
}
/* 水波纹特效：对页面所有 button 生效（事件委托，重渲染后依然有效） */
function spawnRipple(el,ev){
  if(!el)return;
  var r=el.getBoundingClientRect();
  var x=ev?(ev.clientX-r.left):r.width/2;
  var y=ev?(ev.clientY-r.top):r.height/2;
  var s=Math.max(r.width,r.height);
  var sp=document.createElement('span');sp.className='ripple';
  sp.style.width=sp.style.height=s+'px';
  sp.style.left=(x-s/2)+'px';sp.style.top=(y-s/2)+'px';
  el.appendChild(sp);setTimeout(function(){sp.remove();},600);
}
document.addEventListener('click',function(ev){
  var b=ev.target.closest?ev.target.closest('button'):(ev.target.tagName==='BUTTON'?ev.target:null);
  if(b)spawnRipple(b,ev);
});
load();setInterval(load,3000);
/* 文件/文件夹控制按钮：事件委托 + data 属性，避免双重 URL 编码导致指令失效 */
document.addEventListener('click',function(ev){
  var btn=ev.target.closest?ev.target.closest('.btn[data-action]'):null;
  if(!btn)return;
  ev.preventDefault();ev.stopPropagation();
  var action=btn.getAttribute('data-action');
  var file=btn.getAttribute('data-file');
  ctrl(action,file,btn);
});
</script>
</body></html>"""

class RequestHandler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        pass

    def _send(self, code, body, ctype="application/json"):
        if isinstance(body, str):
            body = body.encode("utf-8")
        try:
            self.send_response(code)
            self.send_header("Content-Type", ctype + "; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            # 客户端（浏览器）在我们写出响应前已经断开连接——常见于
            # 用户切走 tab / 浏览器取消请求 / 网络抖动。不影响功能，
            # 静默忽略避免 traceback 刷屏。
            return

    def do_GET(self):
        if self.path in ("/", "/index.html"):
            self._send(200, make_html(), "text/html")
        elif self.path == "/status":
            try:
                with open(STATUS_FILE, "r", encoding="utf-8") as f:
                    data = f.read()
            except Exception:
                data = '{"updated":"-","state":"starting","folders":[],"tasks":[]}'
            self._send(200, data)
        else:
            self._send(404, '{"error":"not found"}')

    def do_POST(self):
        if self.path.startswith("/control"):
            q = urllib.parse.urlparse(self.path).query
            p = urllib.parse.parse_qs(q)
            action = p.get("action", [""])[0]
            file_arg = p.get("file", [""])[0]
            poll_arg = p.get("poll", [""])[0]
            if action:
                # 所有控制指令都留痕：排查"点了没反应"时，先看日志有没有这一行
                log(f">>> HTTP 控制请求: action={action} file={file_arg or '-'}"
                    + (" poll=1" if poll_arg == "1" else ""))
                result = engine.handle_control(action, file_arg or None, poll=(poll_arg == "1"))
                if not result.get("ok"):
                    log(f">>> 指令失败: {action} -> {result.get('err')}")
                self._send(200, json.dumps(result, ensure_ascii=False))
                return
        self._send(404, '{"error":"not found"}')


def http_server():
    # ThreadingHTTPServer：每个请求一个线程，避免单线程下一个慢请求
    # 阻塞后续所有请求导致浏览器超时断连（BrokenPipeError）。
    server = ThreadingHTTPServer(("0.0.0.0", PORT), RequestHandler)
    server.daemon_threads = True
    log("监控服务已启动: http://0.0.0.0:%d (ThreadingHTTPServer)" % PORT)
    server.serve_forever()


# -----------------------------------------------------------------------------
# 入口
# -----------------------------------------------------------------------------

if __name__ == "__main__":
    if not FOLDERS:
        log("错误：未配置 FOLDERS 环境变量")
        exit(1)
    if not os.path.isdir(SRC_BASE):
        log(f"错误：源挂载路径 {SRC_BASE} 不存在")
        exit(1)

    engine = SyncEngine()

    def _on_stop_signal(signum, _frame):
        # 信号处理器只跑在主线程，此时主线程可能正持有 self.lock——
        # 处理器里绝不能再取锁（v1.2.10 死锁教训的变体）。
        # _graceful_stop_rsyncs 只读任务表 + terminate，无锁安全。
        # 整个收尾须在 Docker SIGKILL 前（默认 10s 宽限）完成，限 7 秒。
        log(f">>> 收到退出信号({signum})，优雅停止：等 rsync 保存断点…")
        try:
            engine._graceful_stop_rsyncs(max_wait=7.0)
        except Exception:
            pass
        os._exit(0)

    signal.signal(signal.SIGTERM, _on_stop_signal)
    signal.signal(signal.SIGINT, _on_stop_signal)
    threading.Thread(target=http_server, daemon=True).start()
    threading.Thread(target=engine.save_status_loop, daemon=True).start()
    threading.Thread(target=engine.scheduler, daemon=True).start()
    threading.Thread(target=engine.watchdog_loop, daemon=True).start()
    engine.main_loop()
    log("引擎退出，容器将自动重启")
    exit(0)
