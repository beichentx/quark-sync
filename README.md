# quark-sync

夸克网盘（Quark Drive）→ NAS 单向下载同步引擎，自带 Web 监控面板。**单文件 Python（engine.py），纯标准库，无 pip 依赖。**

> 从个人 fnOS NAS 生产环境提炼开源，同引擎支撑影视库自动化下载链路长期稳定运行。

## 功能

- 定时巡检夸克网盘挂载目录，把新增/未下完的文件同步到本地目标目录
- Web 监控面板：任务树、进度、速度、失败原因，端口可用 `PORT` 修改
- 自愈能力：停滞检测自动重启任务、龟速任务让位、连续失败自动放弃等下轮
- 可配置删源策略（下载完成后删源/保留/静默期）、目录过滤、广告文件过滤
- fnOS/fnOS FUSE 缓存刷新（mountmgr/rclone SIGHUP），非 NAS 环境 no-op

## 快速开始（Docker Compose）

```bash
git clone https://github.com/<your-github-username>/quark-sync.git
cd quark-sync
cp .env.example .env    # 按需修改 SRC_BASE / DST_BASE / PORT
docker compose up -d
```

打开 `http://<NAS_IP>:49999` 看监控面板。

> 本地构建镜像：把 compose 里的 `image:` 行换成 `build: .` 后 `docker compose up -d --build`。

## 参数（环境变量）

| 变量 | 默认 | 说明 |
|---|---|---|
| `SRC_BASE` | `/mnt/quark` | 夸克网盘挂载目录（源，建议容器内只读；路径必须已存在，否则引擎拒绝启动） |
| `DST_BASE` | `/sync/quark` | 下载目标目录 |
| `FOLDERS` | **必填** | 只同步的源目录下子目录名，空格分隔（缺省引擎拒绝启动） |
| `INTERVAL` | `600` | 巡检间隔（秒） |
| `CONCURRENT` | `5` | 并发下载任务数 |
| `PORT` | `49999` | Web 监控端口 |
| `STATE_DIR` | `/tmp/quark_sync` | 状态目录（容器内部；跨重启持久请挂 `/state` 或此目录） |
| `DELETE` | `false` | 下载完成后删除源文件 |
| `DELETE_SRC` / `DELETE_SRC_DIR` / `DELETE_SRC_QUIET_DAYS` | `false`/`false`/`0` | 删源与静默期细控 |
| `MAX_RETRY` | `2` | 单任务重试次数 |
| `STALL_SECONDS` / `STALL_RETRY` | `180`/`3` | 停滞判定与放弃阈值 |
| `LOW_SPEED_BPS` / `LOW_SPEED_SECONDS` | `307200`/`300` | 龟速让位阈值 |
| `PUID` / `PGID` | `1000`/`1001` | 容器内运行用户（root 启动自动降权） |
| `TZ` | `Asia/Shanghai` | 时区 |

其余细分参数见 `engine.py` 头部配置区（均有注释）。

## 部署示例

**fnOS**：夸克挂载点与媒体库均按实际路径填 `SRC_BASE`/`DST_BASE`；建议 compose 显式声明 subnet；`PUID/PGID` 用 `1000:1001`。

**群晖**：套件 `Cloud Sync` 不适用时，用 rclone 挂夸克到本地目录再接本引擎；`PUID/PGID` 填 `1024:101`（administrators 组）按需调整。

**通用 Linux**：`docker compose up -d` 即可；无 FUSE 挂载环境时缓存刷新逻辑自动 no-op。

## 源码直跑（不装 Docker）

```bash
python3 engine.py    # 全部配置走环境变量，见参数表
```

要求 Python 3.8+，依赖 rsync（同步动作）与 curl（健康检查可选）。

## License

MIT
