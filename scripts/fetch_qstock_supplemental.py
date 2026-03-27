from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from ashare_harness_v2.data_harness.supplemental_bridge import (  # noqa: E402
    build_qstock_payload,
    filter_supported_stock_codes,
    json_default,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch supplemental A-share data through qstock-compatible adapters.")
    parser.add_argument("--as-of", required=True, help="Trading date tag.")
    parser.add_argument("--codes", nargs="*", default=[], help="Security codes to enrich.")
    parser.add_argument("--timeout-seconds", type=float, default=15.0, help="Provider timeout.")
    args = parser.parse_args()

    payload = build_qstock_payload(
        args.as_of,
        filter_supported_stock_codes(args.codes),
        timeout_seconds=max(args.timeout_seconds, 5.0),
    )
    json.dump(payload, sys.stdout, ensure_ascii=False, indent=2, default=json_default)


if __name__ == "__main__":
    main()
