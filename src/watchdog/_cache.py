"""磁盘缓存：按日缓存 akshare 全市场数据到 parquet。

同一天内多次调用直接读盘，跨 brief/ask/truth 命令共享。

用法：
    from watchdog._cache import cached_market_df
    df = cached_market_df("ggcg_em_全部", "20260422", lambda: ak.stock_ggcg_em(symbol="全部"))

设计：
  - cache_key 要包含参数（symbol/date 等），否则不同调用会串
  - 文件命名：<key>__<date>.parquet
  - 过期策略：同日命中即用；跨日不命中重拉
  - 失败兜底：读/写 parquet 失败（列类型混合等），退回调用 fn() 不缓存，别让缓存把主逻辑干挂
"""
from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Callable, Optional

import pandas as pd


logger = logging.getLogger(__name__)

_DEFAULT_CACHE_DIR = Path(
    os.environ.get(
        "WATCHDOG_CACHE_DIR",
        "/Users/fqyuki/Documents/kd_2026/自学内容/代码类/ashare-watchdog/data/cache",
    )
)


def _sanitize(s: str) -> str:
    # 文件名里不要有奇怪符号
    safe = "".join(c if c.isalnum() or c in "._-" else "_" for c in s)
    return safe[:120]


def _cache_path(key: str, date_str: str, cache_dir: Optional[Path] = None) -> Path:
    d = cache_dir or _DEFAULT_CACHE_DIR
    return d / f"{_sanitize(key)}__{_sanitize(date_str)}.parquet"


def cached_market_df(
    key: str,
    date_str: str,
    fn: Callable[[], pd.DataFrame],
    cache_dir: Optional[Path] = None,
) -> pd.DataFrame:
    """同日缓存 fn() 返回的 DataFrame。

    命中 → 直接读 parquet；miss → 调 fn()、落盘、返回。
    读/写失败就绕过缓存，保证主流程不挂。
    """
    path = _cache_path(key, date_str, cache_dir)
    if path.exists():
        try:
            df = pd.read_parquet(path)
            logger.debug("cache hit: %s (%d rows)", path.name, len(df))
            return df
        except Exception as e:
            logger.warning("cache read failed %s: %s", path.name, e)
            # 坏缓存删掉
            try:
                path.unlink()
            except Exception:
                pass

    df = fn()
    if df is None:
        return df

    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        # akshare 返回的列可能有混合类型，parquet 写入需要字符串化
        df_to_save = df.copy()
        for col in df_to_save.columns:
            if df_to_save[col].dtype == "object":
                df_to_save[col] = df_to_save[col].astype(str)
        df_to_save.to_parquet(path, index=False)
        logger.debug("cache write: %s (%d rows)", path.name, len(df))
    except Exception as e:
        logger.warning("cache write failed %s: %s", path.name, e)

    return df
