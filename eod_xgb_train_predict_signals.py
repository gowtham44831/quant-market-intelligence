from __future__ import annotations

import os
import json
import socket
import logging
import re
from datetime import timedelta, date
from typing import Optional, List, Tuple

import pendulum
import psycopg2
from psycopg2.extras import execute_values
import pandas as pd
import numpy as np
import xgboost as xgb
import joblib

from airflow import DAG
from airflow.operators.python import PythonOperator

# -----------------------------------------------------------------------------
# LOGGING
# -----------------------------------------------------------------------------
logger = logging.getLogger("airflow.task")
logger.setLevel(logging.INFO)

# -----------------------------------------------------------------------------
# CONFIG
# -----------------------------------------------------------------------------
LOCAL_TZ = pendulum.timezone("America/New_York")

DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "postgres"),
    "port": int(os.getenv("POSTGRES_PORT", 5432)),
    "dbname": os.getenv("POSTGRES_DB", "stocks"),
    "user": os.getenv("POSTGRES_USER", "postgres"),
    "password": os.getenv("POSTGRES_PASSWORD"),
}

MODEL_DIR = os.getenv("MODEL_DIR", "/opt/airflow/data/models")

# -----------------------------------------------------------------------------
# STRATEGY IDENTITY
# -----------------------------------------------------------------------------
# This script implements the FORWARD-RETURN strategy only:
#   labels = sign of forward_return_7d / forward_return_20d
# It must never read or write the triple-barrier strategy's model names,
# feature configuration, or artifacts.
STRATEGY = "forward_return"
LABEL_VERSION = "forward_return_sign_v2"
FEATURE_VERSION = "forward_base_v2"

# Model artifact names (per-leg). Distinct from triple-barrier's xgb_tb_*_v1.
MODEL_NAME_UP = os.getenv("MODEL_NAME_UP", "xgb_forward_up_v2")
MODEL_NAME_DOWN = os.getenv("MODEL_NAME_DOWN", "xgb_forward_down_v2")

# One name to store predictions/signals under (paired up+down)
PREDICTION_SYSTEM_NAME = os.getenv("PREDICTION_SYSTEM_NAME", "xgb_forward_v2")

# Base feature tables
FEATURE_TABLE_120 = os.getenv("FEATURE_TABLE_120", "daily_features_120d")
FEATURE_TABLE_240 = os.getenv("FEATURE_TABLE_240", "daily_features_240d")

HORIZON_CONFIG = {
    7: FEATURE_TABLE_120,
    20: FEATURE_TABLE_240,
}

# Base features
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

FEATURES_BY_HORIZON = {
    7: BASE_FEATURE_COLS_7D,
    20: BASE_FEATURE_COLS_20D,
}

LABEL_COL_BY_HORIZON = {
    7: "forward_return_7d",
    20: "forward_return_20d",
}

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

# Training window
TRAIN_LOOKBACK_DAYS = int(os.getenv("TRAIN_LOOKBACK_DAYS", "730"))
MIN_TRAIN_ROWS = int(os.getenv("MIN_TRAIN_ROWS", "2000"))

_SQL_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def validate_identifier(value: str, setting_name: str) -> str:
    """Allow configured SQL identifiers, but reject executable SQL fragments."""
    if not _SQL_IDENTIFIER.fullmatch(value):
        raise ValueError(f"Invalid SQL identifier for {setting_name}: {value!r}")
    return value


for _setting_name, _identifier in (
    ("FEATURE_TABLE_120", FEATURE_TABLE_120),
    ("FEATURE_TABLE_240", FEATURE_TABLE_240),
    *[(f"feature column {column}", column) for column in set(BASE_FEATURE_COLS_7D + BASE_FEATURE_COLS_20D)],
    *[(f"label column {column}", column) for column in LABEL_COL_BY_HORIZON.values()],
):
    validate_identifier(_identifier, _setting_name)

# -----------------------------------------------------------------------------
# DB helpers
# -----------------------------------------------------------------------------
def get_conn():
    return psycopg2.connect(**DB_CONFIG)

def fetch_spy_qqq_tickers(conn) -> List[str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT ticker
            FROM tickers_info
            WHERE sp500 = TRUE OR qqq = TRUE
            ORDER BY ticker
            """
        )
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

def get_common_trade_dates(conn, start: date, end: date) -> List[date]:
    sql_120 = f"""
      SELECT trade_date
      FROM {FEATURE_TABLE_120}
      WHERE trade_date BETWEEN %s AND %s
      GROUP BY trade_date
    """
    sql_240 = f"""
      SELECT trade_date
      FROM {FEATURE_TABLE_240}
      WHERE trade_date BETWEEN %s AND %s
      GROUP BY trade_date
    """
    d120 = pd.read_sql(sql_120, conn, params=(start, end))["trade_date"].tolist()
    d240 = pd.read_sql(sql_240, conn, params=(start, end))["trade_date"].tolist()
    common = sorted(set(d120).intersection(set(d240)))

    out: List[date] = []
    for d in common:
        out.append(d.date() if hasattr(d, "date") else d)
    return out


def get_training_cutoff_date(conn, table_name: str, as_of_date: date, horizon_days: int) -> date:
    """Find the row whose h-trading-day forward return ends at as_of_date."""
    validate_identifier(table_name, "feature table")
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT DISTINCT trade_date
            FROM {table_name}
            WHERE trade_date <= %s
            ORDER BY trade_date DESC
            OFFSET %s LIMIT 1
            """,
            (as_of_date, horizon_days),
        )
        row = cur.fetchone()
    if row is None:
        raise ValueError(
            f"Not enough trading dates in {table_name} through {as_of_date} "
            f"for a {horizon_days}-trading-day label"
        )
    return row[0]

# -----------------------------------------------------------------------------
# Data loading
# -----------------------------------------------------------------------------
def load_frame_joined(
    conn,
    table_name: str,
    base_cols: List[str],
    label_col: Optional[str],
    min_date: date,
    max_date: date,
    tickers: Optional[List[str]] = None,
) -> pd.DataFrame:
    """
    Loads:
      - base features from f
      - optional label as label_return
    """
    validate_identifier(table_name, "feature table")
    for column in base_cols:
        validate_identifier(column, "feature column")
    if label_col:
        validate_identifier(label_col, "label column")

    base_select = ", ".join([f"f.{c}" for c in base_cols])
    label_select = f", f.{label_col} AS label_return" if label_col else ""

    tickers_filter = ""
    params = [min_date, max_date]
    if tickers is not None:
        tickers_filter = " AND f.ticker = ANY(%s) "
        params.append(tickers)

    sql = f"""
        SELECT
          f.trade_date,
          f.ticker,
          {base_select}
          {label_select}
        FROM {table_name} f
        WHERE f.trade_date BETWEEN %s AND %s
          {tickers_filter}
        ORDER BY f.ticker ASC, f.trade_date ASC
    """
    return pd.read_sql(sql, conn, params=tuple(params))

def clean_frame(df: pd.DataFrame, feature_cols: List[str]) -> pd.DataFrame:
    """Coerce features while leaving missing values for XGBoost to handle."""
    df = df.copy().replace([np.inf, -np.inf], np.nan)
    for c in feature_cols:
        if c not in df.columns:
            raise ValueError(f"Missing required feature column: {c}")
        df[c] = pd.to_numeric(df[c], errors="coerce")

    # XGBoost supports NaN natively. Filling would either leak future values
    # through bfill or risk copying values between ticker groups.
    return df

# -----------------------------------------------------------------------------
# Labels
# -----------------------------------------------------------------------------
def build_up_label(df: pd.DataFrame) -> pd.Series:
    y = pd.to_numeric(df["label_return"], errors="coerce")
    return (y > 0).astype(int)

def build_down_label(df: pd.DataFrame) -> pd.Series:
    y = pd.to_numeric(df["label_return"], errors="coerce")
    return (y < 0).astype(int)

# -----------------------------------------------------------------------------
# XGBoost
# -----------------------------------------------------------------------------
def train_xgb_classifier(X: pd.DataFrame, y: pd.Series, scale_pos_weight: Optional[float] = None) -> xgb.XGBClassifier:
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
    e.g. xgb_forward_up_v2_7d_2026-09-01.joblib
    """
    return os.path.join(MODEL_DIR, f"{model_name}_{horizon_days}d_{as_of_date}.joblib")

def upsert_model_registry(
    conn,
    model_name: str,
    horizon_days: int,
    feature_table: str,
    trained_for_date: date,
    model_path: str,
    feature_cols: List[str],
    train_start_date: date,
    train_end_date: date,
    train_rows: int,
    run_mode: str = "production",
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

# -----------------------------------------------------------------------------
# DB upserts
# -----------------------------------------------------------------------------
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

def upsert_daily_trade_signals(
    conn,
    as_of_date: date,
    horizon_days: int,
    strategy: str,
    df: pd.DataFrame,
    prediction_model_name: str = PREDICTION_SYSTEM_NAME,
    replace_existing: bool = False,
):
    """
    Writes the OFFICIAL production signal. One row per
    (as_of_date, ticker, horizon_days, strategy); model_name records which model
    version produced it and is intentionally not part of the conflict key.
    """
    rows = [] if df is None else [
        (
            as_of_date,
            r["ticker"],
            horizon_days,
            strategy,
            prediction_model_name,
            r["signal"],
            float(r["prob_up"]),
            float(r["score"]),
            None if pd.isna(r.get("volume_zscore_30")) else float(r["volume_zscore_30"]),
            None if pd.isna(r.get("reason")) else str(r["reason"]),
        )
        for _, r in df.iterrows()
    ]

    sql = """
        INSERT INTO daily_trade_signals
          (as_of_date, ticker, horizon_days, strategy, model_name,
           signal, prob_up, score, volume_zscore_30, reason)
        VALUES %s
        ON CONFLICT (as_of_date, ticker, horizon_days, strategy)
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
        if replace_existing:
            cur.execute(
                """
                DELETE FROM daily_trade_signals
                WHERE as_of_date = %s AND horizon_days = %s AND strategy = %s
                """,
                (as_of_date, horizon_days, strategy),
            )
        if rows:
            execute_values(cur, sql, rows, page_size=1000)
    conn.commit()

# -----------------------------------------------------------------------------
# Signal logic (strict gates)
# -----------------------------------------------------------------------------
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

        # The two independently trained legs can disagree. Do not silently give
        # SELL precedence when both pass their respective confidence gates.
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

def build_topn(signals_for_db: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    buy_df = signals_for_db[signals_for_db["signal"] == "BUY"].copy()
    sell_df = signals_for_db[signals_for_db["signal"] == "SELL"].copy()

    top_buy = buy_df.sort_values("score", ascending=False).head(TOPN) if not buy_df.empty else buy_df
    top_sell = sell_df.sort_values("score", ascending=False).head(TOPN) if not sell_df.empty else sell_df
    return top_buy, top_sell


def load_latest_production_pair(conn, horizon_days: int, as_of_date: date):
    """Load the newest complete UP/DOWN pair registered for production."""
    sql = """
        SELECT
          up.trained_for_date,
          up.feature_table,
          up.feature_cols_json,
          up.model_path AS up_path,
          down.model_path AS down_path
        FROM model_registry up
        JOIN model_registry down
          ON down.horizon_days = up.horizon_days
         AND down.trained_for_date = up.trained_for_date
         AND down.strategy = up.strategy
        WHERE up.model_name = %s
          AND down.model_name = %s
          AND up.horizon_days = %s
          AND up.strategy = %s
          AND up.run_mode = 'production'
          AND down.run_mode = 'production'
          AND up.trained_for_date <= %s
        ORDER BY up.trained_for_date DESC
        LIMIT 1
    """
    with conn.cursor() as cur:
        cur.execute(
            sql,
            (MODEL_NAME_UP, MODEL_NAME_DOWN, horizon_days, STRATEGY, as_of_date),
        )
        row = cur.fetchone()

    if row is None:
        raise ValueError(
            f"No complete production model pair registered for horizon={horizon_days} "
            f"through {as_of_date}. Run the training DAG first."
        )

    trained_for_date, feature_table, feature_cols_json, up_path, down_path = row
    feature_cols = (
        json.loads(feature_cols_json)
        if isinstance(feature_cols_json, str)
        else list(feature_cols_json)
    )
    expected_cols = FEATURES_BY_HORIZON[horizon_days]
    if feature_cols != expected_cols:
        raise ValueError(
            f"Registered feature mismatch for horizon={horizon_days}: "
            f"expected={expected_cols}, registered={feature_cols}"
        )
    if feature_table != HORIZON_CONFIG[horizon_days]:
        raise ValueError(
            f"Registered feature table mismatch for horizon={horizon_days}: "
            f"expected={HORIZON_CONFIG[horizon_days]}, registered={feature_table}"
        )
    for path in (up_path, down_path):
        if not os.path.isfile(path):
            raise FileNotFoundError(f"Registered model artifact does not exist: {path}")

    return trained_for_date, feature_table, feature_cols, joblib.load(up_path), joblib.load(down_path)


def predict_with_registered_models() -> None:
    """Score the latest feature date without fitting or modifying any model."""
    conn = get_conn()
    try:
        tickers = fetch_spy_qqq_tickers(conn)
        if not tickers:
            raise ValueError("No S&P 500 or QQQ tickers found in tickers_info.")
        as_of_date = get_latest_common_as_of_date(conn)

        for horizon_days in HORIZON_CONFIG:
            trained_date, table_name, feature_cols, model_up, model_down = (
                load_latest_production_pair(conn, horizon_days, as_of_date)
            )
            df_latest = load_frame_joined(
                conn=conn,
                table_name=table_name,
                base_cols=feature_cols,
                label_col=None,
                min_date=as_of_date,
                max_date=as_of_date,
                tickers=tickers,
            )
            if df_latest.empty:
                raise ValueError(f"No scoring features for horizon={horizon_days} date={as_of_date}")

            df_latest = clean_frame(df_latest, feature_cols)
            preds = pd.DataFrame({
                "ticker": df_latest["ticker"].values,
                "prob_up": model_up.predict_proba(df_latest[feature_cols])[:, 1],
                "prob_down": model_down.predict_proba(df_latest[feature_cols])[:, 1],
            })
            preds["score"] = preds["prob_up"]
            versioned_name = f"{PREDICTION_SYSTEM_NAME}@{trained_date}"
            upsert_predictions(conn, as_of_date, horizon_days, versioned_name, preds)

            signals = compute_signals_dual(preds, df_latest, horizon_days)
            signals_for_db = signals.drop(columns=["prob_down"])
            upsert_daily_trade_signals(
                conn, as_of_date, horizon_days, STRATEGY_ALL, signals_for_db,
                prediction_model_name=versioned_name,
            )
            top_buy, top_sell = build_topn(signals_for_db)
            upsert_daily_trade_signals(
                conn, as_of_date, horizon_days, STRATEGY_TOP10_BUY, top_buy,
                prediction_model_name=versioned_name, replace_existing=True,
            )
            upsert_daily_trade_signals(
                conn, as_of_date, horizon_days, STRATEGY_TOP10_SELL, top_sell,
                prediction_model_name=versioned_name, replace_existing=True,
            )
            logger.info(
                "Predicted h=%d as_of=%s with production model trained_for=%s rows=%d",
                horizon_days, as_of_date, trained_date, len(preds),
            )
    finally:
        conn.close()

# -----------------------------------------------------------------------------
# Core runner (used by BOTH daily + backfill DAGs)
# -----------------------------------------------------------------------------
def run_walk_forward(
    do_backfill: bool,
    backfill_days: int,
    train_only: bool = False,
    registry_run_mode: str = "production",
) -> None:
    """
    Walk-forward training:
      For each as_of_date:
        - Find the h-trading-day cutoff whose forward label ends at as_of_date
        - Train over the configured lookback through that cutoff
        - Score exactly as_of_date
        - Save models named with as_of_date
        - Upsert preds + signals + topN strategies into DB

    Daily DAG: do_backfill=False  -> runs only latest common date
    Backfill DAG: do_backfill=True -> runs N historical dates
    """
    logger.info(
        "RUNNING_ON_HOST=%s do_backfill=%s backfill_days=%d MODEL_DIR=%s",
        socket.gethostname(), do_backfill, backfill_days, MODEL_DIR
    )

    os.makedirs(MODEL_DIR, exist_ok=True)
    conn = get_conn()
    try:
        tickers = fetch_spy_qqq_tickers(conn)
        if not tickers:
            raise ValueError("No S&P 500 or QQQ tickers found in tickers_info.")
        latest_as_of = get_latest_common_as_of_date(conn)

        if do_backfill:
            # Pull enough calendar range to get N trading dates in common
            start = latest_as_of - timedelta(days=backfill_days * 2)
            common_dates = get_common_trade_dates(conn, start, latest_as_of)
            as_of_dates = common_dates[-backfill_days:] if len(common_dates) > backfill_days else common_dates
            if not as_of_dates:
                raise ValueError("No common dates available for backfill.")
            logger.info("Backfill walk-forward: %d dates (%s .. %s)", len(as_of_dates), as_of_dates[0], as_of_dates[-1])
        else:
            as_of_dates = [latest_as_of]
            logger.info("Daily walk-forward: latest only (%s)", latest_as_of)

        for as_of_date in as_of_dates:
            logger.info("=== as_of_date=%s ===", as_of_date)

            for horizon_days, table_name in HORIZON_CONFIG.items():
                base_cols = BASE_FEATURE_COLS_7D if horizon_days == 7 else BASE_FEATURE_COLS_20D
                feature_cols = FEATURES_BY_HORIZON[horizon_days]
                label_col = LABEL_COL_BY_HORIZON[horizon_days]

                # The labels are built with pandas shift(-h), so h means trading
                # rows, not calendar days. This cutoff guarantees every training
                # label ends no later than the as-of date.
                train_max_date = get_training_cutoff_date(
                    conn, table_name, as_of_date, horizon_days
                )
                train_min_date = train_max_date - timedelta(days=TRAIN_LOOKBACK_DAYS)

                if train_max_date <= train_min_date:
                    logger.warning("Skipping horizon=%d date=%s: invalid train window.", horizon_days, as_of_date)
                    continue

                # Training data (with labels)
                df_train = load_frame_joined(
                    conn=conn,
                    table_name=table_name,
                    base_cols=base_cols,
                    label_col=label_col,
                    min_date=train_min_date,
                    max_date=train_max_date,
                    tickers=tickers,
                )

                if df_train.empty:
                    logger.warning("No training rows horizon=%d as_of=%s", horizon_days, as_of_date)
                    continue

                df_train["label_return"] = pd.to_numeric(df_train["label_return"], errors="coerce")
                df_train = df_train.dropna(subset=["label_return"]).copy()

                if len(df_train) < MIN_TRAIN_ROWS:
                    logger.warning("Too few training rows horizon=%d as_of=%s rows=%d (MIN_TRAIN_ROWS=%d)",
                                   horizon_days, as_of_date, len(df_train), MIN_TRAIN_ROWS)
                    continue

                df_train = clean_frame(df_train, feature_cols)
                train_df = df_train

                X_train = train_df[feature_cols]
                y_train_up = build_up_label(train_df)
                y_train_down = build_down_label(train_df)

                if y_train_up.nunique() < 2 or y_train_down.nunique() < 2:
                    logger.warning(
                        "Skipping horizon=%d as_of=%s: training labels contain only one class",
                        horizon_days,
                        as_of_date,
                    )
                    continue

                # imbalance for DOWN
                pos = int((y_train_down == 1).sum())
                neg = int((y_train_down == 0).sum())
                spw = (neg / max(pos, 1))

                # Train and save models for this date+horizon
                model_up = train_xgb_classifier(X_train, y_train_up, scale_pos_weight=None)
                up_path = model_path_for_date(MODEL_NAME_UP, horizon_days, as_of_date)
                joblib.dump(model_up, up_path)

                model_down = train_xgb_classifier(X_train, y_train_down, scale_pos_weight=spw)
                down_path = model_path_for_date(MODEL_NAME_DOWN, horizon_days, as_of_date)
                joblib.dump(model_down, down_path)

                for reg_model_name, reg_path in ((MODEL_NAME_UP, up_path), (MODEL_NAME_DOWN, down_path)):
                    upsert_model_registry(
                        conn,
                        model_name=reg_model_name,
                        horizon_days=horizon_days,
                        feature_table=table_name,
                        trained_for_date=as_of_date,
                        model_path=reg_path,
                        feature_cols=feature_cols,
                        train_start_date=train_min_date,
                        train_end_date=train_max_date,
                        train_rows=len(train_df),
                        run_mode=registry_run_mode,
                    )

                if train_only:
                    logger.info(
                        "TRAINED ONLY h=%dd trained_for=%s rows=%d models=(%s,%s)",
                        horizon_days, as_of_date, len(train_df), up_path, down_path,
                    )
                    continue

                # Latest features for scoring (NO label)
                df_latest = load_frame_joined(
                    conn=conn,
                    table_name=table_name,
                    base_cols=base_cols,
                    label_col=None,
                    min_date=as_of_date,
                    max_date=as_of_date,
                    tickers=tickers,
                )

                if df_latest.empty:
                    logger.warning("No latest features horizon=%d date=%s", horizon_days, as_of_date)
                    continue

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

                upsert_predictions(conn, as_of_date, horizon_days, PREDICTION_SYSTEM_NAME, preds)

                signals_all = compute_signals_dual(preds, df_latest, horizon_days)
                signals_for_db = signals_all.drop(columns=["prob_down"])

                upsert_daily_trade_signals(conn, as_of_date, horizon_days, STRATEGY_ALL, signals_for_db)

                top_buy, top_sell = build_topn(signals_for_db)
                # Replace these materialized top-N views so reruns cannot leave
                # behind tickers that have dropped out of the latest ranking.
                upsert_daily_trade_signals(
                    conn, as_of_date, horizon_days, STRATEGY_TOP10_BUY,
                    top_buy, replace_existing=True,
                )
                upsert_daily_trade_signals(
                    conn, as_of_date, horizon_days, STRATEGY_TOP10_SELL,
                    top_sell, replace_existing=True,
                )

                logger.info(
                    "DONE h=%dd date=%s train=%s..%s train_rows=%d preds=%d buys=%d sells=%d holds=%d "
                    "top_buy=%d top_sell=%d models=(%s,%s)",
                    horizon_days, as_of_date, train_min_date, train_max_date, len(train_df), len(preds),
                    int((signals_for_db["signal"] == "BUY").sum()),
                    int((signals_for_db["signal"] == "SELL").sum()),
                    int((signals_for_db["signal"] == "HOLD").sum()),
                    len(top_buy), len(top_sell),
                    up_path, down_path
                )

        logger.info("ALL DONE walk-forward.")
    finally:
        conn.close()

# -----------------------------------------------------------------------------
# Airflow callables
# -----------------------------------------------------------------------------
def daily_walkforward(**_):
    predict_with_registered_models()


def train_forward_models(**_):
    run_walk_forward(
        do_backfill=False,
        backfill_days=0,
        train_only=True,
        registry_run_mode="production",
    )

# -----------------------------------------------------------------------------
# DAGs
# -----------------------------------------------------------------------------
default_args = {"owner": "airflow", "retries": 1, "retry_delay": timedelta(minutes=5)}

# WEEKLY TRAINING DAG: refreshes the production model after Friday's features.
with DAG(
    dag_id="eod_xgb_forward_train_weekly",
    start_date=pendulum.datetime(2025, 12, 20, tz=LOCAL_TZ),
    schedule="0 18 * * 0",  # Sunday 6:00 PM ET
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["ml", "xgboost", "training", "forward_return", "production"],
) as dag_train:
    PythonOperator(
        task_id="train_and_register_forward_models",
        python_callable=train_forward_models,
    )

# DAILY INFERENCE DAG: loads the latest registered production model; never trains.
with DAG(
    dag_id="eod_xgb_forward_predict_daily",
    start_date=pendulum.datetime(2025, 12, 20, tz=LOCAL_TZ),
    schedule="30 8,17 * * 1-5",  # 8:30 AM + 5:00 PM ET Mon-Fri
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["production", "ml", "xgboost", "walkforward", "daily", "no_news"],
) as dag_daily:
    PythonOperator(
        task_id="predict_with_latest_registered_model",
        python_callable=daily_walkforward,
    )
