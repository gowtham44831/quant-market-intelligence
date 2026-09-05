from __future__ import annotations

import logging
import math
import os
import socket
from datetime import date, timedelta

import pandas as pd
import pendulum
from psycopg2.extras import execute_values

from airflow import DAG
from airflow.operators.python import PythonOperator

import eod_xgb_train_predict_signals as forward


logger = logging.getLogger("airflow.task")
logger.setLevel(logging.INFO)

LOCAL_TZ = pendulum.timezone("America/New_York")
BACKTEST_DAYS = int(os.getenv("FORWARD_BACKTEST_DAYS", "240"))
RETRAIN_EVERY_TRADING_DAYS = int(os.getenv("FORWARD_BACKTEST_RETRAIN_DAYS", "20"))
ROUND_TRIP_COST_BPS = float(os.getenv("FORWARD_BACKTEST_COST_BPS", "10"))
BACKTEST_MODEL_NAME = os.getenv(
    "FORWARD_BACKTEST_MODEL_NAME", f"{forward.PREDICTION_SYSTEM_NAME}_backtest"
)

# Research-only challenger. These columns already exist in daily_features_* and
# are strictly backward-looking on each scoring date. Keep the production/base
# feature lists untouched until the challenger proves itself out of sample.
CHALLENGER_MODEL_NAME = "xgb_forward_v3_context_backtest"
CHALLENGER_COMMON_FEATURES = [
    "relative_volume_20", "atr_14",
    "distance_to_support_20d_atr", "distance_to_support_60d_atr",
    "distance_to_resistance_20d_atr", "distance_to_resistance_60d_atr",
    "breakdown_20d", "breakdown_60d", "breakout_20d", "breakout_60d",
    "spy_return_1d", "spy_return_5d", "spy_return_20d",
    "spy_trend_50", "spy_volatility_20d",
    "qqq_return_1d", "qqq_return_5d", "qqq_return_20d",
    "qqq_trend_50", "qqq_volatility_20d",
    "sector_return_1d", "sector_return_5d", "sector_return_20d",
    "sector_trend_50", "sector_volatility_20d",
    "stock_minus_spy_5d", "stock_minus_spy_20d",
    "stock_minus_qqq_5d", "stock_minus_qqq_20d",
    "stock_minus_sector_5d", "stock_minus_sector_20d",
]
CHALLENGER_FEATURES_BY_HORIZON = {
    horizon: list(dict.fromkeys([*base_features, *CHALLENGER_COMMON_FEATURES]))
    for horizon, base_features in forward.FEATURES_BY_HORIZON.items()
}
ROBUSTNESS_BACKTEST_DAYS = int(
    os.getenv("FORWARD_ROBUSTNESS_BACKTEST_DAYS", "480")
)
ROBUSTNESS_VARIANTS = (
    {
        "model_name": "xgb_forward_v2_7d_480_backtest",
        "features": forward.FEATURES_BY_HORIZON[7],
    },
    {
        "model_name": "xgb_forward_v3_context_7d_480_backtest",
        "features": CHALLENGER_FEATURES_BY_HORIZON[7],
    },
)


class InsufficientTrainingData(ValueError):
    """The historical panel is not yet large/diverse enough to fit a model."""


def upsert_backtest_signals(
    conn,
    as_of_date: date,
    horizon_days: int,
    strategy: str,
    model_name: str,
    frame: pd.DataFrame,
    replace_existing: bool = False,
) -> None:
    rows = [] if frame is None else [
        (
            as_of_date,
            row["ticker"],
            horizon_days,
            strategy,
            model_name,
            row["signal"],
            float(row["prob_up"]),
            float(row["score"]),
            None if pd.isna(row.get("volume_zscore_30")) else float(row["volume_zscore_30"]),
            None if pd.isna(row.get("reason")) else str(row["reason"]),
        )
        for _, row in frame.iterrows()
    ]
    sql = """
        INSERT INTO backtest_trade_signals
          (as_of_date, ticker, horizon_days, strategy, model_name,
           signal, prob_up, score, volume_zscore_30, reason)
        VALUES %s
        ON CONFLICT (as_of_date, ticker, horizon_days, strategy, model_name)
        DO UPDATE SET
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
                DELETE FROM backtest_trade_signals
                WHERE as_of_date = %s AND horizon_days = %s
                  AND strategy = %s AND model_name = %s
                """,
                (as_of_date, horizon_days, strategy, model_name),
            )
        if rows:
            execute_values(cur, sql, rows, page_size=1000)
    conn.commit()


def load_realized_returns(
    conn, table_name: str, label_col: str, as_of_date: date, tickers: list[str]
) -> pd.DataFrame:
    forward.validate_identifier(table_name, "feature table")
    forward.validate_identifier(label_col, "label column")
    return pd.read_sql(
        f"""
        SELECT ticker, {label_col}::float8 AS realized_return
        FROM {table_name}
        WHERE trade_date = %s AND ticker = ANY(%s)
        """,
        conn,
        params=(as_of_date, tickers),
    )


def upsert_backtest_results(
    conn,
    as_of_date: date,
    horizon_days: int,
    strategy: str,
    model_name: str,
    frame: pd.DataFrame,
) -> None:
    cost = ROUND_TRIP_COST_BPS / 10_000.0
    rows = []
    for _, row in frame.iterrows():
        if pd.isna(row["realized_return"]):
            continue
        signal = str(row["signal"])
        realized = float(row["realized_return"])
        gross = realized if signal == "BUY" else -realized if signal == "SELL" else 0.0
        applied_cost = cost if signal in ("BUY", "SELL") else 0.0
        rows.append(
            (
                as_of_date,
                row["ticker"],
                horizon_days,
                strategy,
                model_name,
                signal,
                realized,
                gross,
                applied_cost,
                gross - applied_cost,
            )
        )
    if not rows:
        return
    with conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO forward_backtest_results
              (as_of_date, ticker, horizon_days, strategy, model_name, signal,
               realized_return, gross_strategy_return, transaction_cost, net_strategy_return)
            VALUES %s
            ON CONFLICT (as_of_date, ticker, horizon_days, strategy, model_name)
            DO UPDATE SET
              signal = EXCLUDED.signal,
              realized_return = EXCLUDED.realized_return,
              gross_strategy_return = EXCLUDED.gross_strategy_return,
              transaction_cost = EXCLUDED.transaction_cost,
              net_strategy_return = EXCLUDED.net_strategy_return,
              created_at = NOW()
            """,
            rows,
            page_size=1000,
        )
    conn.commit()


def train_pair_for_backtest(
    conn,
    tickers: list[str],
    as_of_date: date,
    horizon_days: int,
):
    table_name = forward.HORIZON_CONFIG[horizon_days]
    feature_cols = forward.FEATURES_BY_HORIZON[horizon_days]
    label_col = forward.LABEL_COL_BY_HORIZON[horizon_days]
    training_end = forward.get_training_cutoff_date(
        conn, table_name, as_of_date, horizon_days
    )
    training_start = training_end - timedelta(days=forward.TRAIN_LOOKBACK_DAYS)
    frame = forward.load_frame_joined(
        conn, table_name, feature_cols, label_col,
        training_start, training_end, tickers,
    )
    frame["label_return"] = pd.to_numeric(frame["label_return"], errors="coerce")
    frame = frame.dropna(subset=["label_return"])
    if len(frame) < forward.MIN_TRAIN_ROWS:
        raise InsufficientTrainingData(
            f"Insufficient backtest training rows h={horizon_days} date={as_of_date}: "
            f"{len(frame)} < {forward.MIN_TRAIN_ROWS}"
        )
    frame = forward.clean_frame(frame, feature_cols)
    up_labels = forward.build_up_label(frame)
    down_labels = forward.build_down_label(frame)
    if up_labels.nunique() < 2 or down_labels.nunique() < 2:
        raise InsufficientTrainingData(
            f"Only one training class h={horizon_days} date={as_of_date}"
        )
    positives = int((down_labels == 1).sum())
    negatives = int((down_labels == 0).sum())
    up_model = forward.train_xgb_classifier(frame[feature_cols], up_labels)
    down_model = forward.train_xgb_classifier(
        frame[feature_cols], down_labels, negatives / max(positives, 1)
    )
    return training_end, up_model, down_model


def run_forward_backtest(**_) -> None:
    if BACKTEST_DAYS <= 0 or RETRAIN_EVERY_TRADING_DAYS <= 0:
        raise ValueError("Backtest and retraining day counts must be positive.")

    logger.info(
        "RUNNING_ON_HOST=%s days=%d retrain_every=%d cost_bps=%.2f",
        socket.gethostname(), BACKTEST_DAYS, RETRAIN_EVERY_TRADING_DAYS,
        ROUND_TRIP_COST_BPS,
    )
    conn = forward.get_conn()
    try:
        tickers = forward.fetch_spy_qqq_tickers(conn)
        if not tickers:
            raise ValueError("No S&P 500 or QQQ tickers found in tickers_info.")
        latest = forward.get_latest_common_as_of_date(conn)
        search_start = latest - timedelta(days=max(BACKTEST_DAYS * 3, 730))
        dates = forward.get_common_trade_dates(conn, search_start, latest)
        max_horizon = max(forward.HORIZON_CONFIG)
        if len(dates) <= max_horizon * 2:
            raise ValueError("Not enough common feature dates for a walk-forward backtest.")

        # A scoring date needs enough earlier dates both for the label cutoff and
        # MIN_TRAIN_ROWS, plus max_horizon later dates for realized outcomes.
        # Add two dates as a buffer for incomplete ticker coverage.
        min_training_dates = math.ceil(forward.MIN_TRAIN_ROWS / len(tickers))
        history_reserve = max_horizon + min_training_dates + 2
        if len(dates) <= history_reserve + max_horizon:
            raise ValueError(
                "Not enough retained feature history after reserving training "
                "and realized-outcome windows."
            )
        eligible_dates = dates[history_reserve:-max_horizon]
        score_dates = eligible_dates[-BACKTEST_DAYS:]
        if not score_dates:
            raise ValueError("No fully realized dates are available for backtesting.")
        if len(score_dates) < BACKTEST_DAYS:
            logger.warning(
                "Requested %d backtest dates, but retained feature history supports "
                "only %d after reserving %d prior and %d future dates.",
                BACKTEST_DAYS, len(score_dates), history_reserve, max_horizon,
            )
        models: dict[int, tuple[date, object, object]] = {}
        scored_blocks = 0

        for date_index, as_of_date in enumerate(score_dates):
            for horizon_days, table_name in forward.HORIZON_CONFIG.items():
                if horizon_days not in models or date_index % RETRAIN_EVERY_TRADING_DAYS == 0:
                    try:
                        models[horizon_days] = train_pair_for_backtest(
                            conn, tickers, as_of_date, horizon_days
                        )
                    except InsufficientTrainingData as exc:
                        if horizon_days not in models:
                            logger.warning("Skipping untrainable date: %s", exc)
                            continue
                        logger.warning(
                            "Model refresh skipped; retaining prior h=%d model: %s",
                            horizon_days, exc,
                        )
                trained_through, model_up, model_down = models[horizon_days]
                feature_cols = forward.FEATURES_BY_HORIZON[horizon_days]
                latest_frame = forward.load_frame_joined(
                    conn, table_name, feature_cols, None,
                    as_of_date, as_of_date, tickers,
                )
                if latest_frame.empty:
                    continue
                latest_frame = forward.clean_frame(latest_frame, feature_cols)
                predictions = pd.DataFrame({
                    "ticker": latest_frame["ticker"].values,
                    "prob_up": model_up.predict_proba(latest_frame[feature_cols])[:, 1],
                    "prob_down": model_down.predict_proba(latest_frame[feature_cols])[:, 1],
                })
                predictions["score"] = predictions["prob_up"]
                model_name = f"{BACKTEST_MODEL_NAME}@{trained_through}"
                forward.upsert_predictions(
                    conn, as_of_date, horizon_days, model_name, predictions
                )

                signals = forward.compute_signals_dual(
                    predictions, latest_frame, horizon_days
                )
                signals_for_db = signals.drop(columns=["prob_down"])
                upsert_backtest_signals(
                    conn, as_of_date, horizon_days, forward.STRATEGY_ALL,
                    model_name, signals_for_db,
                )
                top_buy, top_sell = forward.build_topn(signals_for_db)
                upsert_backtest_signals(
                    conn, as_of_date, horizon_days, forward.STRATEGY_TOP10_BUY,
                    model_name, top_buy, replace_existing=True,
                )
                upsert_backtest_signals(
                    conn, as_of_date, horizon_days, forward.STRATEGY_TOP10_SELL,
                    model_name, top_sell, replace_existing=True,
                )

                realized = load_realized_returns(
                    conn, table_name, forward.LABEL_COL_BY_HORIZON[horizon_days],
                    as_of_date, tickers,
                )
                for strategy, strategy_frame in (
                    (forward.STRATEGY_ALL, signals_for_db),
                    (forward.STRATEGY_TOP10_BUY, top_buy),
                    (forward.STRATEGY_TOP10_SELL, top_sell),
                ):
                    result_frame = strategy_frame.merge(realized, on="ticker", how="left")
                    upsert_backtest_results(
                        conn, as_of_date, horizon_days, strategy, model_name, result_frame
                    )
                logger.info(
                    "Backtested h=%d as_of=%s model_trained_through=%s rows=%d",
                    horizon_days, as_of_date, trained_through, len(signals_for_db),
                )
                scored_blocks += 1
        if scored_blocks == 0:
            raise ValueError(
                "No backtest dates had enough ticker coverage to satisfy "
                f"MIN_TRAIN_ROWS={forward.MIN_TRAIN_ROWS}. Extend feature history "
                "or lower MIN_TRAIN_ROWS only after confirming the available universe."
            )
    finally:
        conn.close()


def run_challenger_backtest(**context) -> None:
    """Run the expanded feature set without changing production or baseline state."""
    global BACKTEST_MODEL_NAME

    original_model_name = BACKTEST_MODEL_NAME
    original_features = forward.FEATURES_BY_HORIZON
    try:
        BACKTEST_MODEL_NAME = CHALLENGER_MODEL_NAME
        forward.FEATURES_BY_HORIZON = CHALLENGER_FEATURES_BY_HORIZON
        logger.info(
            "Running context challenger feature_counts=%s",
            {horizon: len(cols) for horizon, cols in CHALLENGER_FEATURES_BY_HORIZON.items()},
        )
        run_forward_backtest(**context)
    finally:
        BACKTEST_MODEL_NAME = original_model_name
        forward.FEATURES_BY_HORIZON = original_features


def run_robustness_backtest(**context) -> None:
    """Run matched baseline/challenger 7-day tests over a longer history."""
    global BACKTEST_DAYS, BACKTEST_MODEL_NAME

    original_days = BACKTEST_DAYS
    original_model_name = BACKTEST_MODEL_NAME
    original_horizon_config = forward.HORIZON_CONFIG
    original_features = forward.FEATURES_BY_HORIZON
    original_labels = forward.LABEL_COL_BY_HORIZON
    try:
        BACKTEST_DAYS = ROBUSTNESS_BACKTEST_DAYS
        forward.HORIZON_CONFIG = {7: forward.FEATURE_TABLE_120}
        forward.LABEL_COL_BY_HORIZON = {7: "forward_return_7d"}
        for variant in ROBUSTNESS_VARIANTS:
            BACKTEST_MODEL_NAME = variant["model_name"]
            forward.FEATURES_BY_HORIZON = {7: variant["features"]}
            logger.info(
                "Running robustness variant model=%s days=%d features=%d",
                BACKTEST_MODEL_NAME,
                BACKTEST_DAYS,
                len(variant["features"]),
            )
            run_forward_backtest(**context)
    finally:
        BACKTEST_DAYS = original_days
        BACKTEST_MODEL_NAME = original_model_name
        forward.HORIZON_CONFIG = original_horizon_config
        forward.FEATURES_BY_HORIZON = original_features
        forward.LABEL_COL_BY_HORIZON = original_labels


default_args = {"owner": "airflow", "retries": 1, "retry_delay": timedelta(minutes=5)}

with DAG(
    dag_id="eod_xgb_forward_walkforward_backtest",
    start_date=pendulum.datetime(2025, 12, 20, tz=LOCAL_TZ),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["ml", "xgboost", "forward_return", "backtest", "research"],
) as dag:
    PythonOperator(
        task_id="run_forward_walkforward_backtest",
        python_callable=run_forward_backtest,
    )


with DAG(
    dag_id="eod_xgb_forward_context_challenger_backtest",
    start_date=pendulum.datetime(2025, 12, 20, tz=LOCAL_TZ),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["ml", "xgboost", "forward_return", "backtest", "research", "challenger"],
) as challenger_dag:
    PythonOperator(
        task_id="run_context_challenger_backtest",
        python_callable=run_challenger_backtest,
    )


with DAG(
    dag_id="eod_xgb_forward_7d_robustness_backtest",
    start_date=pendulum.datetime(2025, 12, 20, tz=LOCAL_TZ),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["ml", "xgboost", "forward_return", "backtest", "research", "robustness"],
) as robustness_dag:
    PythonOperator(
        task_id="run_7d_robustness_backtest",
        python_callable=run_robustness_backtest,
    )
