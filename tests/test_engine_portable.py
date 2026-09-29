# -*- coding: utf-8 -*-
"""AC6：engine.py 可移植性——PORT/STATE_DIR/SRC_BASE 环境变量覆盖 + 缺省值保持生产行为"""
import json, os, subprocess, sys, tempfile, unittest
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent


def import_engine(env_add=(), env_del=()):
    code = ("import json,engine;"
            "print(json.dumps({'port': engine.PORT, 'state': engine.STATE_DIR, 'src': engine.SRC_BASE}))")
    e = {k: v for k, v in os.environ.items() if k not in env_del}
    e.update(env_add)
    r = subprocess.run([sys.executable, '-c', code], capture_output=True, text=True,
                       cwd=str(REPO), env=e, timeout=120)
    assert r.returncode == 0, r.stderr[-800:]
    return json.loads(r.stdout.strip().splitlines()[-1])


class TestPortable(unittest.TestCase):

    def test_ac6_env_overrides(self):
        tmp = tempfile.mkdtemp(prefix='qs_state_')
        d = import_engine(env_add={'PORT': '54321', 'STATE_DIR': tmp, 'SRC_BASE': '/data/in'})
        self.assertEqual(d['port'], 54321)
        self.assertEqual(d['state'], tmp)
        self.assertEqual(d['src'], '/data/in')

    def test_ac6_defaults_preserve_prod_behavior(self):
        d = import_engine(env_del=('PORT', 'STATE_DIR', 'SRC_BASE'))
        self.assertEqual(d['port'], 49999)
        self.assertEqual(d['state'], '/tmp/quark_sync')
        self.assertEqual(d['src'], '/mnt/quark')


if __name__ == '__main__':
    unittest.main(verbosity=2)
