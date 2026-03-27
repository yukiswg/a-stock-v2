from __future__ import annotations

import json
import sys
import tempfile
import threading
from http.server import ThreadingHTTPServer
from pathlib import Path
from urllib.parse import quote
from urllib.request import urlopen
import unittest

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from ashare_harness_v2.config import load_config
from ashare_harness_v2.runtime_harness.web import make_handler
from ashare_harness_v2.utils import write_json


class AdviceApiTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.config = load_config(ROOT / "config/default.toml")

    def test_api_exposes_advice_and_discovery(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            homepage_dir = Path(tmp)
            write_json(homepage_dir / "latest_homepage.json", {"as_of": "2026-03-23", "today_action": "test"})
            handler = make_handler(mode="api", root=homepage_dir, config=self.config)
            server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                question = quote("宁德时代现在能买吗")
                with urlopen(f"http://127.0.0.1:{server.server_port}/api/advice?question={question}&as_of=2026-03-23") as response:
                    advice = json.loads(response.read().decode("utf-8"))
                self.assertEqual(advice["security"]["code"], "300750")
                self.assertIn("factor_analysis", advice)
                self.assertTrue(advice["factor_analysis"]["factor_summary"])
                with urlopen(f"http://127.0.0.1:{server.server_port}/api/discovery?as_of=2026-03-23&limit=3") as response:
                    discovery = json.loads(response.read().decode("utf-8"))
                self.assertEqual(len(discovery["ideas"]), 3)
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=5)


if __name__ == "__main__":
    unittest.main()
