"""
Triple-barrier first-touch labelling.

Single source of truth: both the triple-barrier TRAINING pipeline
(eod_xgb_train_predict_signals_backfill.py) and the EVALUATION pipeline
(eod_xgb_eval_metrics_daily.py) import this function, so the evaluator scores the
model against exactly the objective it was trained on. Do not fork this logic.

Label semantics per row (ticker, trade_date), looking forward `horizon_days` bars:
    label_up       = 1 if the +up_mult*vol barrier was touched first
    label_down     = 1 if the -down_mult*vol barrier was touched first
    label_neutral  = 1 if neither barrier was touched within the horizon
    label_resolved = 1 only if a FULL horizon of future bars was available.

`label_resolved == 0` means the outcome is not knowable yet. Those rows must be
excluded from both training and evaluation; otherwise an unknown outcome is
silently treated as "not up AND not down".
"""
from __future__ import annotations

import numpy as np
import pandas as pd

# Volatility is quoted on a ~20-trading-day (monthly) basis in the feature
# tables, so barrier width is rescaled to the requested horizon.
VOL_BASIS_DAYS = 20.0


def add_triple_barrier_labels(
    df: pd.DataFrame,
    horizon_days: int,
    up_mult: float,
    down_mult: float,
    vol_col: str,
) -> pd.DataFrame:
    """
    Returns `df` (sorted by ticker, trade_date) with label_up / label_down /
    label_neutral / label_resolved columns added.

    Requires columns: ticker, trade_date, close_price, <vol_col>.
    """
    if "close_price" not in df.columns:
        raise ValueError("add_triple_barrier_labels requires close_price in df")
    if vol_col not in df.columns:
        raise ValueError(f"add_triple_barrier_labels requires {vol_col} in df")

    df = df.sort_values(["ticker", "trade_date"]).copy()
    df["close_price"] = pd.to_numeric(df["close_price"], errors="coerce").fillna(0.0)
    df[vol_col] = pd.to_numeric(df[vol_col], errors="coerce").fillna(0.0)

    scale = np.sqrt(max(horizon_days, 1) / VOL_BASIS_DAYS)

    label_up = np.zeros(len(df), dtype=np.int8)
    label_down = np.zeros(len(df), dtype=np.int8)
    label_neutral = np.zeros(len(df), dtype=np.int8)
    label_resolved = np.zeros(len(df), dtype=np.int8)

    # O(1) position lookup
    idx = df.index.to_numpy()
    pos_map = {int(idx_val): pos for pos, idx_val in enumerate(idx)}

    for _tkr, g in df.groupby("ticker", sort=False):
        prices = g["close_price"].to_numpy(dtype=float)
        vols = g[vol_col].to_numpy(dtype=float)
        g_idx = g.index.to_numpy()

        n = len(g)
        if n <= horizon_days:
            continue

        for i in range(0, n - horizon_days):
            p0 = prices[i]
            pos = pos_map[int(g_idx[i])]

            label_resolved[pos] = 1

            if p0 <= 0:
                label_neutral[pos] = 1
                continue

            up_th = up_mult * vols[i] * scale
            dn_th = down_mult * vols[i] * scale

            future = prices[i + 1 : i + 1 + horizon_days]
            rets = (future / p0) - 1.0

            hit_up = np.where(rets >= up_th)[0]
            hit_dn = np.where(rets <= -dn_th)[0]

            if hit_up.size == 0 and hit_dn.size == 0:
                label_neutral[pos] = 1
            elif hit_up.size == 0:
                label_down[pos] = 1
            elif hit_dn.size == 0:
                label_up[pos] = 1
            else:
                if hit_up[0] < hit_dn[0]:
                    label_up[pos] = 1
                else:
                    label_down[pos] = 1

    df["label_up"] = label_up
    df["label_down"] = label_down
    df["label_neutral"] = label_neutral
    df["label_resolved"] = label_resolved
    return df
