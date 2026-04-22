"""watchdog.cli — A 股 watchdog 统一 CLI 入口

5 个子命令：
  - watchdog brief     一键跑完整流程（vix + hotspots + gate + 渲染 md）
  - watchdog hotspots  只跑 6 源热点发现
  - watchdog vix       只跑 VIX 红绿灯
  - watchdog truth     单只票真实性核查
  - watchdog ask       自然语言问单只票（"宁德时代能买吗"）

公共行为：
  - --debug：抛异常时打完整 traceback；默认只 print 一行错误
  - config：顶部 _load_config() 读 config/default.toml + config/tickers.json；
            文件不存在就用 {}，零配置也能跑通（各模块都有 DEFAULT_*）
  - as-of 默认值：datetime.now().strftime("%Y-%m-%d")
  - 人读输出为默认；--json 切换到原始 JSON
"""
from __future__ import annotations

import argparse
import json
import sys
import traceback
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

# 项目根；当 cli.py 位于 src/watchdog/cli.py 时，parents[2] 指向项目根
_PROJECT_ROOT = Path(__file__).resolve().parents[2]
_CONFIG_DIR = _PROJECT_ROOT / "config"


# ---------------------------------------------------------------------------
# 配置加载（零配置也不崩）
# ---------------------------------------------------------------------------


def _load_toml(path: Path) -> Dict[str, Any]:
    """优先 tomllib（py3.11+）→ tomli → 静默返回 {}。"""
    if not path.exists():
        return {}
    try:
        import tomllib  # type: ignore[attr-defined]
    except ImportError:
        try:
            import tomli as tomllib  # type: ignore[no-redef]
        except ImportError:
            print(
                f"⚠️  未安装 tomllib / tomli，跳过 {path.name}",
                file=sys.stderr,
            )
            return {}
    try:
        with path.open("rb") as fh:
            return tomllib.load(fh)
    except Exception as exc:
        print(f"⚠️  解析 {path.name} 失败，忽略：{exc}", file=sys.stderr)
        return {}


def _load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        print(f"⚠️  解析 {path.name} 失败，忽略：{exc}", file=sys.stderr)
        return {}


def load_config() -> Dict[str, Any]:
    """合并 config/default.toml + config/tickers.json → 一个 dict。
    任一不存在 / 解析失败都用 {} 降级，不崩。
    tickers.json 的内容塞到返回 dict 的顶层（tickers_tech / tickers_defense /
    universe 这些字段由下游模块各自读取）。

    规范化：tickers.json 里 universe.holdings / watch 既可能是 List[str]，也可能是
    List[dict{code,name,exchange}]。下游代码统一当 List[str] 用，这里把 dict 形式扁平化
    成 {"holdings": ["601699", ...], "names": {"601699": "潞安环能", ...}}。
    """
    cfg: Dict[str, Any] = {}
    cfg.update(_load_toml(_CONFIG_DIR / "default.toml"))
    cfg.update(_load_json(_CONFIG_DIR / "tickers.json"))

    uni = cfg.get("universe") or {}
    if isinstance(uni, dict):
        names = dict(uni.get("names") or {})
        def _norm_list(key: str) -> list:
            raw = uni.get(key) or []
            flat: list = []
            for item in raw:
                if isinstance(item, str):
                    code = item.zfill(6) if item.isdigit() else item
                    flat.append(code)
                elif isinstance(item, dict):
                    code = str(item.get("code") or "").zfill(6)
                    if code:
                        flat.append(code)
                        nm = item.get("name")
                        if nm and code not in names:
                            names[code] = nm
            return flat

        for k in ("holdings", "watch", "benchmarks"):
            if k in uni:
                uni[k] = _norm_list(k)
        uni["names"] = names
        cfg["universe"] = uni

    return cfg


def _today_iso() -> str:
    return datetime.now().strftime("%Y-%m-%d")


def _dump_json(obj: Any) -> None:
    # 用 truth/find_hotspots 里的 _json_default 思路
    def default(o):
        if hasattr(o, "isoformat"):
            return o.isoformat()
        if hasattr(o, "item"):
            try:
                return o.item()
            except Exception:
                return str(o)
        return str(o)

    print(json.dumps(obj, ensure_ascii=False, indent=2, default=default))


# ---------------------------------------------------------------------------
# 子命令实现
# ---------------------------------------------------------------------------


def cmd_vix(args: argparse.Namespace) -> int:
    from watchdog.vix import run_vix_signal

    config = load_config()
    result = run_vix_signal(config)

    if args.json:
        _dump_json(result)
        return 0

    if result.get("error"):
        print(f"❌ VIX 信号失败：{result['error']}", file=sys.stderr)
        return 1

    status = result.get("status", "?")
    red = result.get("red_count", 0)
    tech_pct = result.get("tech_pct", 0)
    print(f"🚦 {status}（{red} 红）｜科技仓位 {tech_pct}%")
    print(f"   {result.get('advice', '')}")
    if result.get("override_divergence"):
        print("   ⚠️ 触发中美背离 override")

    emoji = {"red": "🔴", "yellow": "🟡", "green": "🟢"}
    for key, label in (("rsi", "RSI"), ("vix", "VIX"), ("momentum", "动量")):
        s = (result.get("signals") or {}).get(key) or {}
        print(f"   - {label:<4} {emoji.get(s.get('light', ''), '⚪')} {s.get('detail', '')}")
    return 0


def cmd_hotspots(args: argparse.Namespace) -> int:
    from watchdog.find_hotspots import find_hotspots

    config = load_config()
    as_of = args.as_of or _today_iso()
    result = find_hotspots(config=config, as_of=as_of)

    if args.json:
        _dump_json(result)
        return 0

    sources = result.get("sources") or {}
    ok_cnt = sum(1 for v in sources.values() if v.get("ok"))
    fail = [k for k, v in sources.items() if not v.get("ok")]
    themes = result.get("themes") or []
    print(f"🔥 as_of={result.get('as_of')}  数据源 {ok_cnt}/{len(sources)} 可用", end="")
    if fail:
        print(f"（失败：{', '.join(fail)}）")
    else:
        print()

    if not themes:
        print("（本日无汇出主题）")
        return 0

    # strength 排序已经在 find_hotspots 里做过（持仓 > 源数 > 板块涨幅）
    # 这里直接打表格
    print()
    header = f"{'#':<3} {'theme':<16} {'strength':<8} {'sources_hit':<40} tickers"
    print(header)
    print("-" * len(header))
    for i, th in enumerate(themes[:15], 1):
        theme = (th.get("theme") or "")[:16]
        strength = (th.get("strength") or "")[:8]
        srcs = ",".join(th.get("sources_hit") or [])[:40]
        tks = ",".join((th.get("tickers") or [])[:5])
        print(f"{i:<3} {theme:<16} {strength:<8} {srcs:<40} {tks}")
    print()
    # 再把每个 theme 的 summary 列出来
    for i, th in enumerate(themes[:10], 1):
        print(f"  {i}. {th.get('summary', '')}")
    return 0


def cmd_truth(args: argparse.Namespace) -> int:
    from watchdog.truth import score_truth

    config = load_config()
    ticker = str(args.ticker).zfill(6)
    result = score_truth(ticker, name=args.name, config=config)

    if args.json:
        _dump_json(result)
        return 0

    name = result.get("name") or ""
    score = result.get("score", 0)
    verdict = result.get("verdict", "")
    print(f"🔍 {ticker} {name} ⟶ {score}/5 {verdict}")

    evidence = result.get("evidence") or {}
    labels = {
        "announcements": "公告",
        "shareholder_changes": "增减持",
        "dragon_tiger": "龙虎榜",
        "earnings_forecast": "业绩预告",
        "institutional_research": "机构调研",
    }
    for key, label in labels.items():
        ev = evidence.get(key) or {}
        hit = ev.get("hit")
        count = ev.get("count", 0)
        err = ev.get("error")
        mark = "✅" if hit else "❌"
        first_item = ""
        items = ev.get("items") or []
        if items:
            first = items[0]
            # 尽量找个标题/摘要字段
            for cand in ("title", "report_title", "reason", "interpret", "indicator", "who"):
                v = first.get(cand)
                if v:
                    first_item = f" ｜ {str(v)[:50]}"
                    break
        err_s = f" [err:{err[:40]}]" if err else ""
        print(f"  {mark} {label:<6} count={count}{first_item}{err_s}")
    return 0


def cmd_ask(args: argparse.Namespace) -> int:
    from watchdog.ask_stock import ask_stock

    config = load_config()
    result = ask_stock(args.question, config=config)

    if args.json:
        _dump_json(result)
        return 0

    if result.get("error") == "ticker_not_found":
        print(f"❌ 问题里没抽到股票代码：{args.question}", file=sys.stderr)
        return 1

    ticker = result.get("ticker_inferred") or "?"
    name = result.get("name") or ""
    verdict = result.get("verdict", "?")
    conf = result.get("confidence", "?")
    emoji_map = {"buy": "🟢 buy", "hold": "🟡 hold", "sell": "🔴 sell", "avoid": "⛔ avoid"}
    verdict_disp = emoji_map.get(verdict, verdict)
    print(f"📈 {ticker} {name} ⟶ {verdict_disp}（confidence={conf}）")
    print(f"   {result.get('summary', '')}")

    evidence = result.get("evidence") or {}
    pa = evidence.get("price_action") or {}
    if pa:
        parts = []
        for k, label in (("pct_1d", "1日"), ("pct_5d", "5日"), ("pct_20d", "20日")):
            v = pa.get(k)
            if v is not None:
                parts.append(f"{label} {v:+.2f}%")
        if parts:
            print(f"   价格： {' / '.join(parts)}")
    news = evidence.get("recent_news") or []
    notices = evidence.get("fundamentals", {}).get("notices") or []
    if news:
        print(f"   新闻： {len(news)} 条，最新：{(news[0].get('title') or '')[:50]}")
    if notices:
        print(f"   公告： {len(notices)} 条，最新：{(notices[0].get('title') or '')[:50]}")
    return 0


def cmd_brief(args: argparse.Namespace) -> int:
    from watchdog.daily_brief import run_daily_brief

    config = load_config()
    as_of = args.as_of or _today_iso()
    out_dir = args.output_dir

    print(f"🏃 跑 daily brief as_of={as_of} ...", file=sys.stderr)
    result = run_daily_brief(
        config=config,
        as_of=as_of,
        output_dir=out_dir,
        max_tickers=args.max_tickers,
    )
    md = result.get("markdown", "") or ""
    nbytes = len(md.encode("utf-8"))
    elapsed = float(result.get("elapsed_seconds") or 0.0)
    print(
        f"✅ 生成 {result['path']}（{nbytes} bytes, {elapsed:.1f} 秒）"
    )
    return 0


# ---------------------------------------------------------------------------
# 顶层 parser
# ---------------------------------------------------------------------------


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="watchdog",
        description="A 股每日投资预警：VIX 红绿灯 + 6 源热点 + 真实性核查",
    )
    parser.add_argument(
        "--debug", action="store_true", help="出错时打完整 traceback"
    )
    sub = parser.add_subparsers(dest="cmd", required=True)

    # brief
    p_brief = sub.add_parser(
        "brief", help="一键跑完整流程（vix + hotspots + gate + 渲染 md）"
    )
    p_brief.add_argument("--as-of", default=None, help="YYYY-MM-DD，默认今天")
    p_brief.add_argument(
        "--output-dir",
        default=None,
        help="md 落盘目录，默认 data/output/watchdog/",
    )
    p_brief.add_argument(
        "--max-tickers",
        type=int,
        default=5,
        help="gate 核查的 ticker 上限（默认 5，控制耗时）",
    )
    p_brief.set_defaults(func=cmd_brief)

    # hotspots
    p_hs = sub.add_parser("hotspots", help="只跑 6 源热点发现")
    p_hs.add_argument("--as-of", default=None, help="YYYY-MM-DD，默认今天")
    p_hs.add_argument("--json", action="store_true", help="dump 原始 JSON")
    p_hs.set_defaults(func=cmd_hotspots)

    # vix
    p_vix = sub.add_parser("vix", help="只跑 VIX 红绿灯")
    p_vix.add_argument("--json", action="store_true", help="dump 原始 JSON")
    p_vix.set_defaults(func=cmd_vix)

    # truth
    p_tr = sub.add_parser("truth", help="单只票真实性核查")
    p_tr.add_argument("--ticker", required=True, help="6 位 ticker，如 688256")
    p_tr.add_argument("--name", default=None, help="可选名称")
    p_tr.add_argument("--json", action="store_true", help="dump 原始 JSON")
    p_tr.set_defaults(func=cmd_truth)

    # ask
    p_ask = sub.add_parser("ask", help="自然语言问单只票（'宁德时代能买吗'）")
    p_ask.add_argument(
        "--question",
        required=True,
        help="自然语言问题，含股票名或代码",
    )
    p_ask.add_argument("--json", action="store_true", help="dump 原始 JSON")
    p_ask.set_defaults(func=cmd_ask)

    return parser


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()

    try:
        return int(args.func(args) or 0)
    except KeyboardInterrupt:
        print("\n⛔ 用户中断", file=sys.stderr)
        return 130
    except Exception as exc:
        if getattr(args, "debug", False):
            traceback.print_exc()
        cmd = getattr(args, "cmd", "?")
        print(f"❌ {cmd} 失败：{type(exc).__name__}: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
