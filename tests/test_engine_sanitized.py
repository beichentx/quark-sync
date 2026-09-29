# -*- coding: utf-8 -*-
"""AC7：脱敏终检——仓库全部文本文件零敏感命中（品牌注释/内网IP/个人路径/命名空间）"""
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
TEXT_EXTS = {'.py', '.md', '.yml', '.yaml', '.sh', '.example', '.txt', '.cfg', '.ini', ''}

FORBIDDEN = [
    '飞牛',            # 品牌注释（开源版统一 fnOS/NAS 措辞）
    'beichentx',       # 个人 GitHub 命名空间/身份
    '192.168.',        # 内网 IP
    '/vol1', '/vol2',  # 个人 NAS 存储路径
    'kuake-sync',      # 生产目录名
    'D:\\AgentHub', 'C:\\Users',  # 本机路径
]


class TestSanitized(unittest.TestCase):

    def test_ac7_no_sensitive_tokens(self):
        hits = []
        for p in sorted(REPO.rglob('*')):
            if not p.is_file() or '.git' in p.parts or '__pycache__' in p.parts:
                continue
            if p.resolve() == Path(__file__).resolve():
                continue  # 本文件含敏感词字面量（扫描口径本身），不自扫
            if p.suffix not in TEXT_EXTS:
                continue
            try:
                txt = p.read_text(encoding='utf-8')
            except (UnicodeDecodeError, PermissionError):
                continue
            for tok in FORBIDDEN:
                if tok in txt:
                    hits.append(f'{p.name}: {tok}')
        self.assertEqual(hits, [], '敏感命中: ' + '; '.join(hits))


if __name__ == '__main__':
    unittest.main(verbosity=2)
