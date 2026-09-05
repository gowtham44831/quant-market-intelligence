from __future__ import annotations

import os
import json
import socket
import logging
from datetime import date, timedelta

import pendulum
import numpy as np
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import joblib
import xgboost as xgb
from sqlalchemy import create_engine, text

from airflow import DAG
from airflow.operators.python import PythonOperator

# Shared with the evaluation DAG so metrics score the model on its own objective.
from ml_lib.triple_barrier import add_triple_barrier_labels

logger = logging.getLogger("airflow.task")
logger.setLevel(logging.INFO)

# =============================================================================
# CONFIG
# =============================================================================
LOCAL_TZ = pendulum.timezone("America/New_York")

DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "postgres"),
    "port": int(os.getenv("POSTGRES_PORT", "5432")),
    "dbname": os.getenv("POSTGRES_DB", "stocks"),
    "user": os.getenv("POSTGRES_USER", "postgres"),
    "password": os.getenv("POSTGRES_PASSWORD", "postgres"),
}

MODEL_DIR = os.getenv("MODEL_DIR", "/opt/airflow/data/models")

# -----------------------------------------------------------------------------
# STRATEGY IDENTITY
# -----------------------------------------------------------------------------
# This script implements the TRIPLE-BARRIER strategy only:
#   labels = first-touch of a volatility-scaled up/down barrier within the horizon.
# It has its own model names, feature list and label methodology, and must never
# reuse the forward-return strategy's (xgb_forward_*_v2) artifacts or config.
STRATEGY = "triple_barrier"
LABEL_VERSION = "triple_barrier_first_touch_v1"
FEATURE_VERSION = "tb_base_plus_market_v1"

# Walk-forward backfill horizon (research mode only), counted in TRADING days
# because it slices into common_dates - not calendar days. 240 matches the
# deepest feature window the indicators pipeline maintains (daily_features_240d).
#
# This is only an UPPER CAP. The number of dates actually scored is:
#     len(common_dates) - min_required_back
# where common_dates is the INTERSECTION of daily_features_120d and
# daily_features_240d, and min_required_back = TRAIN_LOOKBACK_TRADING
# + 3*max_horizon + 5 = 185 trading days of warm-up history.
#
# Raising this number alone does NOT widen the backtest; the feature tables have
# to go back further first (stock_indicators_dag keeps only a rolling window).
SCORE_BACKFILL_DAYS = int(os.getenv("SCORE_BACKFILL_DAYS", "240"))

# -----------------------------------------------------------------------------
# RUN MODES
# -----------------------------------------------------------------------------
# daily    -> score the latest as-of date, write PRODUCTION daily_trade_signals
# backtest -> walk forward over history, write RESEARCH backtest_trade_signals
#
# The target table is derived from the mode and is never configurable, so a
# research run cannot touch production rows.
MODE_DAILY = "daily"
MODE_BACKTEST = "backtest"

PRODUCTION_SIGNALS_TABLE = "daily_trade_signals"
BACKTEST_SIGNALS_TABLE = "backtest_trade_signals"

# Use SPY/QQQ market-context features (added as extra predictor columns)
USE_MARKET_CONTEXT = os.getenv("USE_MARKET_CONTEXT", "true").lower() == "true"
MARKET_TICKERS = ["SPY", "QQQ"]

# Triple-barrier model artifact names. Distinct from xgb_forward_*_v2.
MODEL_NAME_UP = os.getenv("MODEL_NAME_UP", "xgb_tb_up_v1")
MODEL_NAME_DOWN = os.getenv("MODEL_NAME_DOWN", "xgb_tb_down_v1")

# Prediction/signal model_name. The backtest run uses a suffixed name so research
# rows in model_predictions never overwrite the daily triple-barrier rows.
PREDICTION_SYSTEM_NAME = os.getenv("PREDICTION_SYSTEM_NAME", "xgb_tb_v1")
BACKTEST_PREDICTION_SYSTEM_NAME = os.getenv(
    "BACKTEST_PREDICTION_SYSTEM_NAME", f"{PREDICTION_SYSTEM_NAME}_backtest"
)

FEATURE_TABLE_120 = os.getenv("FEATURE_TABLE_120", "daily_features_120d")
FEATURE_TABLE_240 = os.getenv("FEATURE_TABLE_240", "daily_features_240d")

# FEATURE TABLE DECISION (verified against db_bootstrap_schema.py DDL):
#
#   7d  -> daily_features_120d
#   20d -> daily_features_240d
#
# Rationale:
#  * BASE_FEATURE_COLS_7D uses only columns that daily_features_120d actually has
#    (close_price, vwap, sma_50/100, ema_50/100, rsi_14/30, macd*, volatility_30d/60d,
#    volume_zscore_30). Nothing in the 7d list requires the 240d-only long windows.
#  * BASE_FEATURE_COLS_20D DOES require 240d-only columns (sma_200, ema_200, rsi_60,
#    volatility_120d, volume_zscore_60), so 20d must stay on daily_features_240d.
#  * load_market_features() only needs close_price, rsi_14, macd_hist and
#    volatility_30d, all present in both tables, so the SPY/QQQ context block is
#    valid from either source.
#  * Both tables are written by the same indicatorsCalculation DAG over the same
#    universe, so date coverage matches; the 120d table's indicators are computed
#    on a 120-day window, which is the appropriate scale for a 7-day horizon.
#
# This now matches the forward-return strategy's mapping, so a horizon means the
# same thing in both strategies and only the LABEL differs.
HORIZON_CONFIG = {
    7: FEATURE_TABLE_120,
    20: FEATURE_TABLE_240,
}

# -----------------------------------------------------------------------------
# TRIPLE-BARRIER LABEL CONFIG
# -----------------------------------------------------------------------------
BARRIER_MULT_UP = float(os.getenv("BARRIER_MULT_UP", "1.0"))
BARRIER_MULT_DOWN = float(os.getenv("BARRIER_MULT_DOWN", "1.0"))
BARRIER_VOL_COL = os.getenv("BARRIER_VOL_COL", "volatility_30d")
DROP_NEUTRALS = os.getenv("DROP_NEUTRALS", "false").lower() == "true"

# -----------------------------------------------------------------------------
# FEATURES
# -----------------------------------------------------------------------------
BASE_FEATURE_COLS_7D = [
    "close_price", "vwap",
    "sma_50", "sma_100",
    "ema_50", "ema_100",
    "rsi_14", "rsi_30",
    "macd", "macd_signal", "macd_hist",
    "volatility_30d", "volatility_60d",
    "volume_zscore_30",
]

BASE_FEATURE_COLS_20D = [
    "close_price", "vwap",
    "sma_50", "sma_100", "sma_200",
    "ema_50", "ema_100", "ema_200",
    "rsi_14", "rsi_30", "rsi_60",
    "macd", "macd_signal", "macd_hist",
    "volatility_30d", "volatility_60d", "volatility_120d",
    "volume_zscore_30", "volume_zscore_60",
]

# Market-context columns pulled from SPY/QQQ rows in the same feature table,
# merged onto every ticker's row by trade_date (momentum + regime + volatility).
MARKET_FEATURE_COLS = [
    "spy_return_1d", "spy_rsi_14", "spy_macd_hist", "spy_volatility_30d",
    "qqq_return_1d", "qqq_rsi_14", "qqq_macd_hist", "qqq_volatility_30d",
]

def feature_cols_for_horizon(h: int) -> list[str]:
    cols = list(BASE_FEATURE_COLS_7D if h == 7 else BASE_FEATURE_COLS_20D)
    if USE_MARKET_CONTEXT:
        cols += MARKET_FEATURE_COLS
    return cols

# -----------------------------------------------------------------------------
# SIGNAL CONFIG
# -----------------------------------------------------------------------------
BUY_UP_PROB = float(os.getenv("BUY_UP_PROB", "0.55"))
SELL_DOWN_PROB = float(os.getenv("SELL_DOWN_PROB", "0.52"))
VOL_Z_GATE = float(os.getenv("VOL_Z_GATE", "0.50"))

VOL_Z_CLIP_MAX = float(os.getenv("VOL_Z_CLIP_MAX", "3.0"))
VOL_SCORE_BOOST = float(os.getenv("VOL_SCORE_BOOST", "0.25"))

SELL20_PROB_DOWN = float(os.getenv("SELL20_PROB_DOWN", "0.60"))
SELL20_VOLZ_MIN = float(os.getenv("SELL20_VOLZ_MIN", "1.20"))
SELL20_RSI_MIN = float(os.getenv("SELL20_RSI_MIN", "60.0"))

# Official production strategy row, plus derived top-N views of the same run.
STRATEGY_ALL = os.getenv("SIGNAL_STRATEGY_ALL", STRATEGY)
STRATEGY_TOP10_BUY = os.getenv("SIGNAL_TOP10_BUY", f"{STRATEGY}_top10_buy")
STRATEGY_TOP10_SELL = os.getenv("SIGNAL_TOP10_SELL", f"{STRATEGY}_top10_sell")
TOPN = int(os.getenv("SIGNAL_TOPN", "10"))

# Training window (use trading days to avoid calendar-day leakage)
# 120 TRADING days ~= 6 calendar months (NOT 2 years - the old comment was wrong).
TRAIN_LOOKBACK_TRADING = int(os.getenv("TRAIN_LOOKBACK_TRADING", "120"))
VAL_DAYS = int(os.getenv("VAL_DAYS", "90"))
MIN_TRAIN_ROWS = int(os.getenv("MIN_TRAIN_ROWS", "2000"))

# =============================================================================
# DB HELPERS
# =============================================================================
def get_conn():
    return psycopg2.connect(**DB_CONFIG)

def get_sqlalchemy_engine():
    # ✅ FIX: use SQLAlchemy so pandas stops warning
    uri = (
        f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
    )
    return create_engine(uri, pool_pre_ping=True)

def fetch_spy_qqq_tickers(conn) -> list[str]:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT ticker
            FROM tickers_info
            WHERE sp500 = TRUE OR qqq = TRUE
            ORDER BY ticker
        """)
        return [r[0] for r in cur.fetchall()]

def get_latest_common_as_of_date(conn) -> date:
    with conn.cursor() as cur:
        cur.execute(f"SELECT MAX(trade_date) FROM {FEATURE_TABLE_120}")
        d120 = cur.fetchone()[0]
        cur.execute(f"SELECT MAX(trade_date) FROM {FEATURE_TABLE_240}")
        d240 = cur.fetchone()[0]

    candidates = [d for d in (d120, d240) if d is not None]
    if not candidates:
        raise ValueError("No feature data found (both feature tables empty).")

    as_of = min(candidates)
    logger.info("Latest common as_of_date=%s (120=%s, 240=%s)", as_of, d120, d240)
    return as_of

def get_common_trade_dates(engine, start: date, end: date) -> list[date]:
    sql_120 = text(f"""
        SELECT trade_date
        FROM {FEATURE_TABLE_120}
        WHERE trade_date BETWEEN :start AND :end
        GROUP BY trade_date
    """)
    sql_240 = text(f"""
        SELECT trade_date
        FROM {FEATURE_TABLE_240}
        WHERE trade_date BETWEEN :start AND :end
        GROUP BY trade_date
    """)

    d120 = pd.read_sql(sql_120, engine, params={"start": start, "end": end})["trade_date"].tolist()
    d240 = pd.read_sql(sql_240, engine, params={"start": start, "end": end})["trade_date"].tolist()

    common = sorted(set(d120).intersection(set(d240)))
    out: list[date] = []
    for d in common:
        out.append(d.date() if hasattr(d, "date") else d)
    return out

def shift_trade_date(trade_dates: list[date], anchor: date, shift: int) -> date:
    """
    shift < 0 goes backwards, shift > 0 goes forward.
    """
    try:
        i = trade_dates.index(anchor)
    except ValueError:
        raise ValueError(f"anchor date {anchor} not found in trade_dates")
    j = i + shift
    if j < 0 or j >= len(trade_dates):
        raise ValueError(f"shift out of range: anchor={anchor} shift={shift}")
    return trade_dates[j]

# =============================================================================
# DATA LOADING
# =============================================================================
def load_frame_joined(
    engine,
    table_name: str,
    base_cols: list[str],
    min_date: date,
    max_date: date,
    tickers: list[str] | None,
) -> pd.DataFrame:
    base_select = ", ".join([f"f.{c}" for c in base_cols])

    tickers_filter = ""
    params = {"min_date": min_date, "max_date": max_date}
    if tickers is not None:
        # Using = ANY(:tickers) with SQLAlchemy is annoying; simplest is IN list expansion.
        # We'll keep it safe by only using tickers list from our DB.
        tickers_filter = " AND f.ticker = ANY(:tickers) "
        params["tickers"] = tickers

    sql = text(f"""
        SELECT
          f.trade_date,
          f.ticker,
          {base_select}
        FROM {table_name} f
        WHERE f.trade_date BETWEEN :min_date AND :max_date
          {tickers_filter}
        ORDER BY f.trade_date ASC
    """)

    return pd.read_sql(sql, engine, params=params)

def load_market_features(engine, table_name: str, min_date: date, max_date: date) -> pd.DataFrame:
    """
    Loads SPY/QQQ rows from the same feature table and pivots them into
    wide columns (one row per trade_date) for use as market-context features.

    Fetches a small lookback buffer before min_date so return_1d can be
    computed even when [min_date, max_date] is a single day (scoring case).
    """
    buffered_min_date = min_date - timedelta(days=10)
    sql = text(f"""
        SELECT trade_date, ticker, close_price, rsi_14, macd_hist, volatility_30d
        FROM {table_name}
        WHERE ticker = ANY(:tickers)
          AND trade_date BETWEEN :min_date AND :max_date
        ORDER BY trade_date ASC
    """)
    df = pd.read_sql(sql, engine, params={"tickers": MARKET_TICKERS, "min_date": buffered_min_date, "max_date": max_date})
    if df.empty:
        return pd.DataFrame(columns=["trade_date"] + MARKET_FEATURE_COLS)

    df["close_price"] = pd.to_numeric(df["close_price"], errors="coerce")

    wide_frames = []
    for ticker in MARKET_TICKERS:
        g = df[df["ticker"] == ticker].sort_values("trade_date").copy()
        if g.empty:
            continue
        prefix = ticker.lower()
        g[f"{prefix}_return_1d"] = g["close_price"].pct_change()
        g = g.rename(columns={
            "rsi_14": f"{prefix}_rsi_14",
            "macd_hist": f"{prefix}_macd_hist",
            "volatility_30d": f"{prefix}_volatility_30d",
        })
        cols = ["trade_date", f"{prefix}_return_1d", f"{prefix}_rsi_14", f"{prefix}_macd_hist", f"{prefix}_volatility_30d"]
        wide_frames.append(g[cols])

    if not wide_frames:
        return pd.DataFrame(columns=["trade_date"] + MARKET_FEATURE_COLS)

    market_df = wide_frames[0]
    for wf in wide_frames[1:]:
        market_df = market_df.merge(wf, on="trade_date", how="outer")

    market_df = market_df.sort_values("trade_date").reset_index(drop=True)
    market_df = market_df[
        (market_df["trade_date"] >= min_date) & (market_df["trade_date"] <= max_date)
    ].reset_index(drop=True)
    return market_df

def merge_market_features(df: pd.DataFrame, market_df: pd.DataFrame) -> pd.DataFrame:
    if not USE_MARKET_CONTEXT:
        return df
    if market_df is None or market_df.empty:
        for c in MARKET_FEATURE_COLS:
            df[c] = 0.0
        return df
    return df.merge(market_df, on="trade_date", how="left")

def clean_frame(df: pd.DataFrame, feature_cols: list[str]) -> pd.DataFrame:
    df = df.copy().replace([np.inf, -np.inf], np.nan)
    for c in feature_cols:
        if c not in df.columns:
            raise ValueError(f"Missing required feature column: {c}")
        df[c] = pd.to_numeric(df[c], errors="coerce")
    # XGBoost handles NaN. Do not leak future/cross-ticker values by filling.
    return df

def time_split(df: pd.DataFrame, val_days: int) -> tuple[pd.DataFrame, pd.DataFrame]:
    unique_dates = sorted(df["trade_date"].unique())
    if len(unique_dates) <= val_days + 5:
        return df, df.iloc[0:0].copy()
    cutoff = unique_dates[-val_days]
    return df[df["trade_date"] < cutoff].copy(), df[df["trade_date"] >= cutoff].copy()

# =============================================================================
# XGBOOST
# =============================================================================
def train_xgb_classifier(X: pd.DataFrame, y: pd.Series, scale_pos_weight: float | None = None) -> xgb.XGBClassifier:
    params = dict(
        n_estimators=900,
        max_depth=4,
        learning_rate=0.03,
        subsample=0.8,
        colsample_bytree=0.8,
        reg_lambda=1.0,
        objective="binary:logistic",
        eval_metric="logloss",
        n_jobs=1,
        random_state=42,
        tree_method="hist",
    )
    if scale_pos_weight is not None and scale_pos_weight > 0:
        params["scale_pos_weight"] = float(scale_pos_weight)

    model = xgb.XGBClassifier(**params)
    model.fit(X, y)
    return model

def model_path_for_date(model_name: str, horizon_days: int, as_of_date: date) -> str:
    """
    Artifact path embeds model name (which encodes strategy), horizon and as-of date,
    e.g. xgb_tb_up_v1_7d_2026-09-01.joblib
    """
    return os.path.join(MODEL_DIR, f"{model_name}_{horizon_days}d_{as_of_date}.joblib")

def upsert_model_registry(
    conn,
    model_name: str,
    horizon_days: int,
    feature_table: str,
    trained_for_date: date,
    model_path: str,
    feature_cols: list[str],
    train_start_date: date,
    train_end_date: date,
    train_rows: int,
    run_mode: str,
) -> None:
    sql = """
        INSERT INTO model_registry
          (model_name, horizon_days, feature_table, trained_for_date, model_path,
           strategy, label_version, feature_version, feature_cols_json,
           train_start_date, train_end_date, train_rows, run_mode)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s, %s, %s)
        ON CONFLICT (model_name, horizon_days, trained_for_date)
        DO UPDATE SET
          feature_table = EXCLUDED.feature_table,
          model_path = EXCLUDED.model_path,
          strategy = EXCLUDED.strategy,
          label_version = EXCLUDED.label_version,
          feature_version = EXCLUDED.feature_version,
          feature_cols_json = EXCLUDED.feature_cols_json,
          train_start_date = EXCLUDED.train_start_date,
          train_end_date = EXCLUDED.train_end_date,
          train_rows = EXCLUDED.train_rows,
          run_mode = EXCLUDED.run_mode
    """
    with conn.cursor() as cur:
        cur.execute(
            sql,
            (
                model_name, horizon_days, feature_table, trained_for_date, model_path,
                STRATEGY, LABEL_VERSION, FEATURE_VERSION, json.dumps(feature_cols),
                train_start_date, train_end_date, int(train_rows), run_mode,
            ),
        )
    conn.commit()

# =============================================================================
# DB UPSERTS
# =============================================================================
def upsert_predictions(conn, as_of_date: date, horizon_days: int, model_name: str, preds_df: pd.DataFrame):
    if preds_df is None or preds_df.empty:
        return

    rows = [
        (
            as_of_date,
            horizon_days,
            r["ticker"],
            float(r["prob_up"]),
            float(r["score"]),
            float(r["prob_down"]),
            model_name,
        )
        for _, r in preds_df.iterrows()
    ]

    sql = """
        INSERT INTO model_predictions
          (as_of_date, horizon_days, ticker, prob_up, score, prob_down, model_name)
        VALUES %s
        ON CONFLICT (as_of_date, horizon_days, ticker, model_name)
        DO UPDATE SET
          prob_up = EXCLUDED.prob_up,
          score = EXCLUDED.score,
          prob_down = EXCLUDED.prob_down,
          created_at = NOW()
    """

    with conn.cursor() as cur:
        execute_values(cur, sql, rows, page_size=1000)
    conn.commit()

def upsert_signals(
    conn,
    mode: str,
    as_of_date: date,
    horizon_days: int,
    strategy: str,
    model_name: str,
    df: pd.DataFrame,
):
    """
    Single write path for both modes. The destination table is chosen from `mode`,
    NOT from configuration:

      MODE_DAILY    -> daily_trade_signals    (production)
                       conflict on (as_of_date, ticker, horizon_days, strategy)
      MODE_BACKTEST -> backtest_trade_signals (research)
                       conflict on (as_of_date, ticker, horizon_days, strategy, model_name)

    A backtest run therefore cannot insert into or update production rows.
    """
    if df is None or df.empty:
        return

    if mode == MODE_DAILY:
        table = PRODUCTION_SIGNALS_TABLE
        conflict_cols = "(as_of_date, ticker, horizon_days, strategy)"
    elif mode == MODE_BACKTEST:
        table = BACKTEST_SIGNALS_TABLE
        conflict_cols = "(as_of_date, ticker, horizon_days, strategy, model_name)"
    else:
        raise ValueError(f"Unknown run mode: {mode!r}")

    rows = [
        (
            as_of_date,
            r["ticker"],
            horizon_days,
            strategy,
            model_name,
            r["signal"],
            float(r["prob_up"]),
            float(r["score"]),
            None if pd.isna(r.get("volume_zscore_30")) else float(r["volume_zscore_30"]),
            None if pd.isna(r.get("reason")) else str(r["reason"]),
        )
        for _, r in df.iterrows()
    ]

    sql = f"""
        INSERT INTO {table}
          (as_of_date, ticker, horizon_days, strategy, model_name,
           signal, prob_up, score, volume_zscore_30, reason)
        VALUES %s
        ON CONFLICT {conflict_cols}
        DO UPDATE SET
          model_name = EXCLUDED.model_name,
          signal = EXCLUDED.signal,
          prob_up = EXCLUDED.prob_up,
          score = EXCLUDED.score,
          volume_zscore_30 = EXCLUDED.volume_zscore_30,
          reason = EXCLUDED.reason,
          created_at = NOW()
    """

    with conn.cursor() as cur:
        execute_values(cur, sql, rows, page_size=1000)
    conn.commit()

# =============================================================================
# SIGNAL LOGIC
# =============================================================================
def compute_signals_dual(preds: pd.DataFrame, df_latest: pd.DataFrame, horizon_days: int) -> pd.DataFrame:
    needed = ["ticker", "volume_zscore_30", "rsi_14", "close_price", "sma_50"]
    missing = [c for c in needed if c not in df_latest.columns]
    if missing:
        raise ValueError(f"df_latest missing required columns: {missing}")

    signals = preds.merge(df_latest[needed], on="ticker", how="left")
    for c in ["volume_zscore_30", "rsi_14", "close_price", "sma_50"]:
        signals[c] = pd.to_numeric(signals[c], errors="coerce")

    vol_norm = np.clip(signals["volume_zscore_30"], 0.0, VOL_Z_CLIP_MAX) / max(VOL_Z_CLIP_MAX, 1e-9)
    score_buy = signals["prob_up"] * (1.0 + VOL_SCORE_BOOST * vol_norm)
    score_sell = signals["prob_down"] * (1.0 + VOL_SCORE_BOOST * vol_norm)

    def sell20_gate(prob_down: float, vol_z: float, rsi14: float, close_px: float, sma50: float) -> bool:
        return (
            prob_down >= SELL20_PROB_DOWN and
            vol_z >= SELL20_VOLZ_MIN and
            rsi14 >= SELL20_RSI_MIN and
            close_px < sma50
        )

    def decide(row) -> str:
        prob_up = float(row["prob_up"])
        prob_down = float(row["prob_down"])
        vol_z = float(row["volume_zscore_30"])
        rsi14 = float(row["rsi_14"])
        close_px = float(row["close_price"])
        sma50 = float(row["sma_50"])

        if any(pd.isna(value) for value in (prob_up, prob_down, vol_z, rsi14, close_px, sma50)):
            return "HOLD"
        if vol_z < VOL_Z_GATE:
            return "HOLD"

        buy_eligible = prob_up >= BUY_UP_PROB
        if horizon_days == 20:
            sell_eligible = sell20_gate(prob_down, vol_z, rsi14, close_px, sma50)
        else:
            sell_eligible = prob_down >= SELL_DOWN_PROB

        if buy_eligible and sell_eligible:
            return "HOLD"
        if sell_eligible:
            return "SELL"
        if buy_eligible:
            return "BUY"

        return "HOLD"

    signals["signal"] = signals.apply(decide, axis=1)
    signals["score"] = np.where(signals["signal"] == "SELL", score_sell, score_buy)
    signals["score"] = pd.to_numeric(signals["score"], errors="coerce").fillna(0.0)

    signals["reason"] = signals.apply(
        lambda r: (
            f"h={horizon_days} prob_up={r['prob_up']:.3f} prob_down={r['prob_down']:.3f} "
            f"vol_z30={r['volume_zscore_30']:.2f} rsi14={r['rsi_14']:.1f} "
            f"close={r['close_price']:.2f} sma50={r['sma_50']:.2f} score={r['score']:.3f}"
        ),
        axis=1
    )

    return signals[["ticker", "signal", "prob_up", "prob_down", "score", "volume_zscore_30", "reason"]]

def build_topn(signals_for_db: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    buy_df = signals_for_db[signals_for_db["signal"] == "BUY"].copy()
    sell_df = signals_for_db[signals_for_db["signal"] == "SELL"].copy()
    top_buy = buy_df.sort_values("score", ascending=False).head(TOPN) if not buy_df.empty else buy_df
    top_sell = sell_df.sort_values("score", ascending=False).head(TOPN) if not sell_df.empty else sell_df
    return top_buy, top_sell

# =============================================================================
# MAIN TRIPLE-BARRIER RUNNER (shared by daily + backtest modes)
# =============================================================================
def run_triple_barrier(mode: str, **_):
    if mode not in (MODE_DAILY, MODE_BACKTEST):
        raise ValueError(f"Unknown run mode: {mode!r}")

    # Research runs are namespaced away from the daily model_name so they cannot
    # overwrite daily triple-barrier predictions either.
    prediction_model_name = (
        PREDICTION_SYSTEM_NAME if mode == MODE_DAILY else BACKTEST_PREDICTION_SYSTEM_NAME
    )
    target_table = PRODUCTION_SIGNALS_TABLE if mode == MODE_DAILY else BACKTEST_SIGNALS_TABLE

    logger.info(
        "RUNNING_ON_HOST=%s mode=%s target_table=%s model_name=%s backfill_days=%d MODEL_DIR=%s "
        "barrier(up=%.2f down=%.2f vol=%s) tables=(7:%s,20:%s)",
        socket.gethostname(),
        mode,
        target_table,
        prediction_model_name,
        SCORE_BACKFILL_DAYS,
        MODEL_DIR,
        BARRIER_MULT_UP,
        BARRIER_MULT_DOWN,
        BARRIER_VOL_COL,
        HORIZON_CONFIG[7],
        HORIZON_CONFIG[20],
    )

    os.makedirs(MODEL_DIR, exist_ok=True)
    engine = get_sqlalchemy_engine()
    conn = get_conn()
    try:
        tickers = fetch_spy_qqq_tickers(conn)
        latest_as_of = get_latest_common_as_of_date(conn)

        start_search = latest_as_of - timedelta(days=365 * 6)  # big enough for most datasets
        common_dates = get_common_trade_dates(engine, start_search, latest_as_of)
        if not common_dates:
            raise ValueError("No common trade dates found.")

        # Only as-of dates with enough PAST data to train and label are usable.
        min_required_back = TRAIN_LOOKBACK_TRADING + (max(HORIZON_CONFIG.keys()) * 3) + 5
        safe_dates = [d for idx, d in enumerate(common_dates) if idx >= min_required_back]

        if not safe_dates:
            raise ValueError("No safe as_of_dates available (insufficient history).")

        if mode == MODE_BACKTEST:
            as_of_dates = (
                safe_dates[-SCORE_BACKFILL_DAYS:]
                if len(safe_dates) > SCORE_BACKFILL_DAYS
                else safe_dates
            )
        else:
            # Daily: score only the latest available feature date. Training still
            # ends horizon_days before it, so no barrier outcome is assumed known.
            as_of_dates = [safe_dates[-1]]
            if safe_dates[-1] != latest_as_of:
                logger.warning(
                    "Latest common feature date %s lacks sufficient history; scoring %s instead.",
                    latest_as_of, safe_dates[-1],
                )

        logger.info(
            "mode=%s: %d as_of date(s) (%s .. %s)",
            mode, len(as_of_dates), as_of_dates[0], as_of_dates[-1],
        )

        for as_of_date in as_of_dates:
            logger.info("=== as_of_date=%s ===", as_of_date)

            for horizon_days, table_name in HORIZON_CONFIG.items():
                base_cols = BASE_FEATURE_COLS_7D if horizon_days == 7 else BASE_FEATURE_COLS_20D
                feature_cols = feature_cols_for_horizon(horizon_days)

                # ✅ FIX: avoid look-ahead using TRADING DAY shift (not calendar timedelta)
                training_end = shift_trade_date(common_dates, as_of_date, -horizon_days)

                # Training start as trading lookback
                training_start = shift_trade_date(
                    common_dates,
                    training_end,
                    -min(TRAIN_LOOKBACK_TRADING, common_dates.index(training_end))
                )

                # Need future rows to label up to training_end + horizon (and some buffer)
                label_max = shift_trade_date(
                    common_dates,
                    training_end,
                    min(horizon_days * 3, len(common_dates) - 1 - common_dates.index(training_end))
                )

                df_train = load_frame_joined(
                    engine=engine,
                    table_name=table_name,
                    base_cols=base_cols,
                    min_date=training_start,
                    max_date=label_max,
                    tickers=tickers,
                )
                if df_train.empty:
                    logger.warning("No training rows h=%d as_of=%s", horizon_days, as_of_date)
                    continue

                if USE_MARKET_CONTEXT:
                    market_train = load_market_features(engine, table_name, training_start, label_max)
                    df_train = merge_market_features(df_train, market_train)

                df_train = add_triple_barrier_labels(
                    df_train,
                    horizon_days=horizon_days,
                    up_mult=BARRIER_MULT_UP,
                    down_mult=BARRIER_MULT_DOWN,
                    vol_col=BARRIER_VOL_COL,
                )

                # keep only rows <= training_end for training (labels already use future)
                df_train = df_train[df_train["trade_date"] <= training_end].copy()

                # Drop observations whose barrier outcome cannot yet be known.
                unresolved = int((df_train["label_resolved"] == 0).sum())
                if unresolved:
                    logger.info(
                        "Dropping %d unresolved-barrier training rows h=%d as_of=%s",
                        unresolved, horizon_days, as_of_date,
                    )
                df_train = df_train[df_train["label_resolved"] == 1].copy()

                if DROP_NEUTRALS:
                    df_train = df_train[df_train["label_neutral"] == 0].copy()

                if len(df_train) < MIN_TRAIN_ROWS:
                    logger.warning("Too few training rows h=%d as_of=%s rows=%d", horizon_days, as_of_date, len(df_train))
                    continue

                df_train = clean_frame(df_train, feature_cols)
                train_df = df_train

                X_train = train_df[feature_cols]
                y_train_up = train_df["label_up"].astype(int)
                y_train_down = train_df["label_down"].astype(int)

                # imbalance for DOWN
                pos = int((y_train_down == 1).sum())
                neg = int((y_train_down == 0).sum())
                spw = (neg / max(pos, 1))

                if y_train_up.nunique() < 2 or y_train_down.nunique() < 2:
                    logger.warning("Skipping one-class training set h=%d as_of=%s", horizon_days, as_of_date)
                    continue

                artifact_up_name = MODEL_NAME_UP if mode == MODE_DAILY else f"{MODEL_NAME_UP}_backtest"
                artifact_down_name = MODEL_NAME_DOWN if mode == MODE_DAILY else f"{MODEL_NAME_DOWN}_backtest"

                model_up = train_xgb_classifier(X_train, y_train_up, scale_pos_weight=None)
                up_path = model_path_for_date(artifact_up_name, horizon_days, as_of_date)
                joblib.dump(model_up, up_path)

                model_down = train_xgb_classifier(X_train, y_train_down, scale_pos_weight=spw)
                down_path = model_path_for_date(artifact_down_name, horizon_days, as_of_date)
                joblib.dump(model_down, down_path)

                for reg_model_name, reg_path in ((artifact_up_name, up_path), (artifact_down_name, down_path)):
                    upsert_model_registry(
                        conn,
                        model_name=reg_model_name,
                        horizon_days=horizon_days,
                        feature_table=table_name,
                        trained_for_date=as_of_date,
                        model_path=reg_path,
                        feature_cols=feature_cols,
                        train_start_date=training_start,
                        train_end_date=training_end,
                        train_rows=len(train_df),
                        run_mode=mode,
                    )

                # Score exactly as_of_date
                df_latest = load_frame_joined(
                    engine=engine,
                    table_name=table_name,
                    base_cols=base_cols,
                    min_date=as_of_date,
                    max_date=as_of_date,
                    tickers=tickers,
                )
                if df_latest.empty:
                    logger.warning("No latest features h=%d date=%s", horizon_days, as_of_date)
                    continue

                if USE_MARKET_CONTEXT:
                    market_latest = load_market_features(engine, table_name, as_of_date, as_of_date)
                    df_latest = merge_market_features(df_latest, market_latest)

                df_latest = clean_frame(df_latest, feature_cols)

                X_latest = df_latest[feature_cols]
                prob_up = model_up.predict_proba(X_latest)[:, 1]
                prob_down = model_down.predict_proba(X_latest)[:, 1]

                preds = pd.DataFrame({
                    "ticker": df_latest["ticker"].values,
                    "prob_up": prob_up,
                    "prob_down": prob_down,
                })
                preds["score"] = preds["prob_up"]

                upsert_predictions(conn, as_of_date, horizon_days, prediction_model_name, preds)

                signals_all = compute_signals_dual(preds, df_latest, horizon_days)

                # signal tables don't carry prob_down → keep existing shape
                signals_for_db = signals_all.drop(columns=["prob_down"])

                upsert_signals(
                    conn, mode, as_of_date, horizon_days,
                    STRATEGY_ALL, prediction_model_name, signals_for_db,
                )

                top_buy, top_sell = build_topn(signals_for_db)
                if not top_buy.empty:
                    upsert_signals(
                        conn, mode, as_of_date, horizon_days,
                        STRATEGY_TOP10_BUY, prediction_model_name, top_buy,
                    )
                if not top_sell.empty:
                    upsert_signals(
                        conn, mode, as_of_date, horizon_days,
                        STRATEGY_TOP10_SELL, prediction_model_name, top_sell,
                    )

                logger.info(
                    "DONE h=%dd date=%s train=%s..%s train_rows=%d pos_up=%d pos_down=%d preds=%d models=(%s,%s)",
                    horizon_days,
                    as_of_date,
                    training_start,
                    training_end,
                    len(df_train),
                    int(y_train_up.sum()),
                    int(y_train_down.sum()),
                    len(preds),
                    up_path,
                    down_path,
                )

        logger.info("ALL DONE mode=%s (barrier labels).", mode)
    finally:
        try:
            conn.close()
        except Exception:
            pass
        try:
            engine.dispose()
        except Exception:
            pass

# =============================================================================
# Airflow callables
# =============================================================================
# Mode is bound here, not read from env/params, so a research trigger can never
# be pointed at the production signals table.
def daily_triple_barrier(**_):
    run_triple_barrier(mode=MODE_DAILY)

def backtest_triple_barrier(**_):
    run_triple_barrier(mode=MODE_BACKTEST)

# =============================================================================
# DAGs
# =============================================================================
default_args = {"owner": "airflow", "retries": 1, "retry_delay": timedelta(minutes=5)}

# 1) PRODUCTION: daily triple-barrier signals -> daily_trade_signals
with DAG(
    dag_id="eod_xgb_tb_daily_signals",
    start_date=pendulum.datetime(2025, 12, 20, tz=LOCAL_TZ),
    schedule="30 8,17 * * 1-5",  # 8:30 AM + 5:00 PM ET Mon-Fri (matches forward-return DAG)
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["ml", "xgboost", "daily", "barriers", "production"],
) as dag_tb_daily:
    PythonOperator(
        task_id="daily_triple_barrier",
        python_callable=daily_triple_barrier,
    )

# 2) RESEARCH: walk-forward backtest -> backtest_trade_signals ONLY
with DAG(
    dag_id="eod_xgb_tb_backtest",
    start_date=pendulum.datetime(2025, 12, 20, tz=LOCAL_TZ),
    schedule=None,  # manual only
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["ml", "xgboost", "backtest", "walkforward", "barriers", "research"],
) as dag_tb_backtest:
    PythonOperator(
        task_id="backtest_triple_barrier",
        python_callable=backtest_triple_barrier,
    )
