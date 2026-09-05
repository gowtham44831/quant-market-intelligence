from __future__ import annotations

import os
import json
import socket
import logging
from datetime import timedelta, date

import pendulum
import psycopg2
from psycopg2.extras import execute_values
import pandas as pd
import numpy as np

from sklearn.metrics import roc_auc_score, log_loss, confusion_matrix

from airflow import DAG
from airflow.operators.python import PythonOperator

# Same labelling code the triple-barrier model trains on, so barrier models are
# scored against their own objective rather than a proxy.
from ml_lib.triple_barrier import add_triple_barrier_labels

logger = logging.getLogger("airflow.task")
logger.setLevel(logging.INFO)

LOCAL_TZ = pendulum.timezone("America/New_York")

DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "postgres"),
    "port": int(os.getenv("POSTGRES_PORT", 5432)),
    "dbname": os.getenv("POSTGRES_DB", "stocks"),
    "user": os.getenv("POSTGRES_USER", "postgres"),
    "password": os.getenv("POSTGRES_PASSWORD"),
}

# What to evaluate.
# Each production strategy has its own model_name; metrics are always keyed by
# (as_of_date, horizon_days, model_name, strategy) so forward-return and
# triple-barrier results are never aggregated together.
EVAL_MODEL_NAMES = [
    x.strip()
    for x in os.getenv(
        "EVAL_MODEL_NAMES", "xgb_forward_v2,xgb_forward_v2_backtest,xgb_tb_v1,xgb_tb_v1_backtest"
    ).split(",")
    if x.strip()
]
EVAL_LOOKBACK_DAYS = int(os.getenv("EVAL_LOOKBACK_DAYS", "240"))

# Also evaluate research walk-forward output from backtest_trade_signals.
# Those rows carry their own model_name (…_backtest), so they land on separate
# primary keys and can never merge with production metrics.
EVAL_INCLUDE_BACKTEST = os.getenv("EVAL_INCLUDE_BACKTEST", "true").lower() == "true"

# -----------------------------------------------------------------------------
# LABEL BASIS PER MODEL
# -----------------------------------------------------------------------------
# Forward-return models are scored on sign(forward_return_Nd).
# Triple-barrier models are scored on their OWN objective: did the up barrier get
# touched first. Scoring a barrier model on forward-return sign understates it,
# because a first-touch win and a positive end-of-window return are different
# events. Both bases are recorded in model_eval_metrics_daily.label_basis.
LABEL_BASIS_FORWARD = "forward_return_sign"
LABEL_BASIS_BARRIER = "triple_barrier_first_touch"

BARRIER_MODEL_NAMES = {
    x.strip()
    for x in os.getenv("BARRIER_MODEL_NAMES", "xgb_tb_v1,xgb_tb_v1_backtest").split(",")
    if x.strip()
}

# MUST mirror the triple-barrier training config, otherwise the evaluator would
# grade against barriers the model was never trained on.
BARRIER_MULT_UP = float(os.getenv("BARRIER_MULT_UP", "1.0"))
BARRIER_MULT_DOWN = float(os.getenv("BARRIER_MULT_DOWN", "1.0"))
BARRIER_VOL_COL = os.getenv("BARRIER_VOL_COL", "volatility_30d")


def label_basis_for_model(model_name: str) -> str:
    base_name = model_name.split("@", 1)[0]
    return LABEL_BASIS_BARRIER if base_name in BARRIER_MODEL_NAMES else LABEL_BASIS_FORWARD

# If you want to only evaluate specific strategies, comma-separate them.
# Example: "forward_return,triple_barrier"
EVAL_STRATEGIES = os.getenv("EVAL_STRATEGIES", "").strip()

# Threshold for confusion matrix / classification metrics
EVAL_THRESHOLD = float(os.getenv("EVAL_THRESHOLD", "0.50"))

# Calibration bins
CALIB_BINS = int(os.getenv("CALIB_BINS", "10"))

# Feature tables for realized forward returns
FEATURE_TABLE_120 = os.getenv("FEATURE_TABLE_120", "daily_features_120d")
FEATURE_TABLE_240 = os.getenv("FEATURE_TABLE_240", "daily_features_240d")


def get_conn():
    return psycopg2.connect(**DB_CONFIG)


def _safe_date(x) -> date:
    return x.date() if hasattr(x, "date") else x


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
    return _safe_date(as_of)


def _table_exists(conn, table_name: str) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass(%s) IS NOT NULL", (f"public.{table_name}",))
        return bool(cur.fetchone()[0])


def _signals_source_sql(signals_table: str, source_label: str) -> str:
    """
    One prediction/signal source. model_name is part of the join key: without it,
    one strategy's predictions would be paired with another strategy's signals.
    """
    return f"""
        SELECT
          p.as_of_date,
          p.horizon_days,
          p.ticker,
          p.model_name,
          p.prob_up::float8 AS prob_up,
          COALESCE(p.prob_down::float8, 0.0) AS prob_down,
          s.strategy,
          s.signal,
          '{source_label}'::text AS source
        FROM model_predictions p
        JOIN {signals_table} s
          ON s.as_of_date = p.as_of_date
         AND s.horizon_days = p.horizon_days
         AND s.ticker = p.ticker
         AND s.model_name = p.model_name
        WHERE p.as_of_date BETWEEN %s AND %s
          AND split_part(p.model_name, '@', 1) = ANY(%s)
    """


def load_barrier_labels(conn, horizon_days: int, feature_table: str,
                        start_date: date, end_date: date) -> pd.DataFrame:
    """
    Realized triple-barrier outcome per (as_of_date, ticker) for one horizon,
    computed with the SAME function the model trains on (ml_lib.triple_barrier).

    Rows whose barrier window is not fully in the past come back with
    barrier_resolved=0 and are excluded from metrics.
    """
    raw = pd.read_sql(
        f"""
        SELECT trade_date, ticker,
               close_price::float8 AS close_price,
               {BARRIER_VOL_COL}::float8 AS {BARRIER_VOL_COL}
        FROM {feature_table}
        WHERE trade_date BETWEEN %s AND %s
        """,
        conn,
        params=(start_date, end_date),
    )
    if raw.empty:
        return pd.DataFrame(
            columns=["as_of_date", "ticker", "horizon_days", "barrier_label_up", "barrier_resolved"]
        )

    labelled = add_triple_barrier_labels(
        raw,
        horizon_days=horizon_days,
        up_mult=BARRIER_MULT_UP,
        down_mult=BARRIER_MULT_DOWN,
        vol_col=BARRIER_VOL_COL,
    )

    out = labelled[["trade_date", "ticker", "label_up", "label_resolved"]].rename(
        columns={
            "trade_date": "as_of_date",
            "label_up": "barrier_label_up",
            "label_resolved": "barrier_resolved",
        }
    )
    out["horizon_days"] = horizon_days
    return out


def load_eval_frame(conn, start_date: date, end_date: date) -> pd.DataFrame:
    """
    Returns rows at ticker-level:
      as_of_date, horizon_days, ticker, model_name, strategy, signal, source,
      prob_up, label_return, barrier_label_up, barrier_resolved

    Production (daily_trade_signals) and research (backtest_trade_signals) rows are
    unioned but stay separable: research rows carry their own model_name.
    """
    # 1) predictions joined to signals (will duplicate per strategy; that is what we want)
    sources = [("daily_trade_signals", "production")]
    if EVAL_INCLUDE_BACKTEST:
        sources.append(("backtest_trade_signals", "backtest"))

    strategies = [x.strip() for x in EVAL_STRATEGIES.split(",") if x.strip()] if EVAL_STRATEGIES else None

    frames = []
    for signals_table, source_label in sources:
        # Tolerate a not-yet-bootstrapped backtest table instead of failing the run.
        if not _table_exists(conn, signals_table):
            logger.warning("Skipping %s: table does not exist (run db_schema_bootstrap).", signals_table)
            continue

        sql = _signals_source_sql(signals_table, source_label)
        params = [start_date, end_date, EVAL_MODEL_NAMES]
        if strategies:
            sql += " AND s.strategy = ANY(%s) "
            params.append(strategies)
        part = pd.read_sql(sql, conn, params=tuple(params))
        logger.info("Loaded %d rows from %s", len(part), signals_table)
        if not part.empty:
            frames.append(part)

    if not frames:
        return pd.DataFrame()

    df = pd.concat(frames, ignore_index=True)

    # 2) load realized label_return from feature tables
    # horizon 7 -> forward_return_7d from 120d
    # horizon 20 -> forward_return_20d from 240d
    lab7 = pd.read_sql(
        f"""
        SELECT trade_date AS as_of_date, ticker,
               forward_return_7d::float8 AS label_return
        FROM {FEATURE_TABLE_120}
        WHERE trade_date BETWEEN %s AND %s
        """,
        conn,
        params=(start_date, end_date),
    )

    lab20 = pd.read_sql(
        f"""
        SELECT trade_date AS as_of_date, ticker,
               forward_return_20d::float8 AS label_return
        FROM {FEATURE_TABLE_240}
        WHERE trade_date BETWEEN %s AND %s
        """,
        conn,
        params=(start_date, end_date),
    )

    lab7["horizon_days"] = 7
    lab20["horizon_days"] = 20
    labs = pd.concat([lab7, lab20], ignore_index=True)

    # Normalize dates
    df["as_of_date"] = df["as_of_date"].map(_safe_date)
    labs["as_of_date"] = labs["as_of_date"].map(_safe_date)

    out = df.merge(labs, on=["as_of_date", "horizon_days", "ticker"], how="left")
    out["label_return"] = pd.to_numeric(out["label_return"], errors="coerce")

    # 3) realized barrier outcomes, only if a barrier model is actually present
    needs_barrier = out["model_name"].isin(BARRIER_MODEL_NAMES).any()
    if needs_barrier:
        barrier_frames = [
            load_barrier_labels(conn, 7, FEATURE_TABLE_120, start_date, end_date),
            load_barrier_labels(conn, 20, FEATURE_TABLE_240, start_date, end_date),
        ]
        barriers = pd.concat([b for b in barrier_frames if not b.empty], ignore_index=True) \
            if any(not b.empty for b in barrier_frames) else pd.DataFrame()
        if not barriers.empty:
            barriers["as_of_date"] = barriers["as_of_date"].map(_safe_date)
            out = out.merge(barriers, on=["as_of_date", "horizon_days", "ticker"], how="left")

    if "barrier_label_up" not in out.columns:
        out["barrier_label_up"] = np.nan
        out["barrier_resolved"] = 0

    out["barrier_resolved"] = pd.to_numeric(out["barrier_resolved"], errors="coerce").fillna(0).astype(int)
    return out


def calibration_bins(y_true: np.ndarray, p: np.ndarray, n_bins: int = 10):
    """
    Returns (ece, bins_json)
    bins_json: list of dicts with bin edges, count, mean_pred, frac_pos
    """
    p = np.clip(p, 1e-9, 1 - 1e-9)
    bins = np.linspace(0.0, 1.0, n_bins + 1)

    rows = []
    ece = 0.0
    n = len(p)

    for i in range(n_bins):
        lo, hi = bins[i], bins[i + 1]
        # include right edge in last bin
        if i == n_bins - 1:
            mask = (p >= lo) & (p <= hi)
        else:
            mask = (p >= lo) & (p < hi)

        cnt = int(mask.sum())
        if cnt == 0:
            rows.append(
                {"bin": i, "lo": float(lo), "hi": float(hi), "count": 0, "mean_pred": None, "frac_pos": None}
            )
            continue

        mean_pred = float(np.mean(p[mask]))
        frac_pos = float(np.mean(y_true[mask]))
        w = cnt / max(n, 1)
        ece += w * abs(frac_pos - mean_pred)

        rows.append(
            {
                "bin": i,
                "lo": float(lo),
                "hi": float(hi),
                "count": cnt,
                "mean_pred": mean_pred,
                "frac_pos": frac_pos,
            }
        )

    return float(ece), rows


def compute_block_metrics(
    g: pd.DataFrame,
    as_of_date: date,
    horizon_days: int,
    model_name: str,
    strategy: str,
    label_basis: str,
    source: str,
):
    """
    Compute metrics for one (as_of_date, horizon_days, model_name, strategy) block.

    `label_basis` decides what "correct" means for this model:
      forward_return_sign        -> y_true = forward_return > 0
      triple_barrier_first_touch -> y_true = up barrier touched first,
                                    restricted to rows with a resolved barrier.

    Returns a tuple matching model_eval_metrics_daily columns.
    """
    gg = g.dropna(subset=["prob_up"]).copy()

    if label_basis == LABEL_BASIS_BARRIER:
        gg = gg[(gg["barrier_resolved"] == 1) & gg["barrier_label_up"].notna()].copy()
        if gg.empty:
            return None
        y_true = gg["barrier_label_up"].astype(int).values
    else:
        gg = gg.dropna(subset=["label_return"]).copy()
        if gg.empty:
            return None
        y_true = (gg["label_return"].astype(float) > 0).astype(int).values

    n_rows = int(len(gg))
    if n_rows == 0:
        return None

    p = np.clip(gg["prob_up"].astype(float).values, 1e-9, 1 - 1e-9)

    positive_rate = float(np.mean(y_true))

    # AUC only if both classes exist
    auc = None
    if len(np.unique(y_true)) == 2:
        auc = float(roc_auc_score(y_true, p))

    ll = float(log_loss(y_true, p, labels=[0, 1]))
    brier = float(np.mean((p - y_true) ** 2))

    ece, bins_json = calibration_bins(y_true, p, n_bins=CALIB_BINS)

    # Confusion matrix at threshold
    y_pred = (p >= EVAL_THRESHOLD).astype(int)
    tn, fp, fn, tp = confusion_matrix(y_true, y_pred, labels=[0, 1]).ravel()

    # PnL always uses REALIZED FORWARD RETURN regardless of label basis: that is
    # the actual money outcome, independent of how the model defines a win.
    # BUY => +label_return, SELL => -label_return, HOLD => 0
    sig = gg["signal"].astype(str).values
    ret = pd.to_numeric(gg["label_return"], errors="coerce").fillna(0.0).astype(float).values

    pnl = np.zeros_like(ret, dtype=float)
    pnl[sig == "BUY"] = ret[sig == "BUY"]
    pnl[sig == "SELL"] = -ret[sig == "SELL"]

    pnl_sum = float(np.sum(pnl))
    pnl_avg = float(np.mean(pnl)) if n_rows else 0.0

    n_buys = int((sig == "BUY").sum())
    n_sells = int((sig == "SELL").sum())
    n_holds = int((sig == "HOLD").sum())

    return (
        as_of_date,
        int(horizon_days),
        str(model_name),
        str(strategy),
        str(label_basis),
        str(source),
        n_rows,
        positive_rate,
        auc,
        ll,
        brier,
        ece,
        json.dumps(bins_json),
        int(tn),
        int(fp),
        int(fn),
        int(tp),
        float(EVAL_THRESHOLD),
        pnl_avg,
        pnl_sum,
        n_buys,
        n_sells,
        n_holds,
    )


def upsert_metrics(conn, rows: list[tuple]):
    """
    Upsert into model_eval_metrics_daily.
    IMPORTANT: dedupe rows within this batch to avoid
    'ON CONFLICT DO UPDATE command cannot affect row a second time'.
    """
    if not rows:
        return

    # Deduplicate by PK: (as_of_date, horizon_days, model_name, strategy)
    dedup = {}
    for r in rows:
        key = (r[0], r[1], r[2], r[3])
        dedup[key] = r
    rows = list(dedup.values())

    upsert_sql = """
        INSERT INTO model_eval_metrics_daily (
          as_of_date, horizon_days, model_name, strategy,
          label_basis, source,
          n_rows, positive_rate, auc, logloss, brier, ece,
          calib_bins_json,
          tn, fp, fn, tp,
          threshold,
          pnl_avg, pnl_sum,
          n_buys, n_sells, n_holds
        )
        VALUES %s
        ON CONFLICT (as_of_date, horizon_days, model_name, strategy)
        DO UPDATE SET
          label_basis = EXCLUDED.label_basis,
          source = EXCLUDED.source,
          n_rows = EXCLUDED.n_rows,
          positive_rate = EXCLUDED.positive_rate,
          auc = EXCLUDED.auc,
          logloss = EXCLUDED.logloss,
          brier = EXCLUDED.brier,
          ece = EXCLUDED.ece,
          calib_bins_json = EXCLUDED.calib_bins_json,
          tn = EXCLUDED.tn,
          fp = EXCLUDED.fp,
          fn = EXCLUDED.fn,
          tp = EXCLUDED.tp,
          threshold = EXCLUDED.threshold,
          pnl_avg = EXCLUDED.pnl_avg,
          pnl_sum = EXCLUDED.pnl_sum,
          n_buys = EXCLUDED.n_buys,
          n_sells = EXCLUDED.n_sells,
          n_holds = EXCLUDED.n_holds,
          created_at = NOW()
    """

    with conn.cursor() as cur:
        execute_values(cur, upsert_sql, rows, page_size=500)
    conn.commit()


def compute_and_store_metrics(**_):
    logger.info(
        "RUNNING_ON_HOST=%s model_names=%s EVAL_LOOKBACK_DAYS=%d threshold=%.3f bins=%d strategies=%s",
        socket.gethostname(),
        ",".join(EVAL_MODEL_NAMES),
        EVAL_LOOKBACK_DAYS,
        EVAL_THRESHOLD,
        CALIB_BINS,
        (EVAL_STRATEGIES or "<ALL>"),
    )

    conn = get_conn()
    try:
        end_date = get_latest_common_as_of_date(conn)
        start_date = end_date - timedelta(days=EVAL_LOOKBACK_DAYS * 2)  # buffer for non-trading days
        # We'll later group by actual dates present in data.

        df = load_eval_frame(conn, start_date, end_date)
        if df.empty:
            logger.warning("No evaluation rows found for window %s..%s", start_date, end_date)
            return

        # Use only dates within last EVAL_LOOKBACK_DAYS distinct as_of_dates (trading days)
        unique_dates = sorted(df["as_of_date"].unique())
        if len(unique_dates) > EVAL_LOOKBACK_DAYS:
            keep = set(unique_dates[-EVAL_LOOKBACK_DAYS:])
            df = df[df["as_of_date"].isin(keep)].copy()

        unique_dates = sorted(df["as_of_date"].unique())
        logger.info("Evaluating window: %s .. %s (%d dates, %d rows)",
                    unique_dates[0], unique_dates[-1], len(unique_dates), len(df))

        # Ensure types
        df["horizon_days"] = pd.to_numeric(df["horizon_days"], errors="coerce").astype(int)
        df["prob_up"] = pd.to_numeric(df["prob_up"], errors="coerce")
        df["label_return"] = pd.to_numeric(df["label_return"], errors="coerce")

        results_rows: list[tuple] = []

        # Grouping includes model_name AND source, so production and research rows
        # for the same strategy are never averaged into one metric.
        group_keys = ["as_of_date", "horizon_days", "model_name", "strategy", "source"]

        # --- per-day metrics ---
        for (as_of_date, horizon_days, model_name, strategy, source), g in df.groupby(
            group_keys, sort=True
        ):
            row = compute_block_metrics(
                g,
                _safe_date(as_of_date),
                int(horizon_days),
                str(model_name),
                str(strategy),
                label_basis_for_model(str(model_name)),
                str(source),
            )
            if row is not None:
                results_rows.append(row)

        # --- window aggregate metrics ---
        # store under strategy="...__window" to avoid colliding with the real per-day metrics on end_date
        window_end = max(unique_dates)
        for (horizon_days, model_name, strategy, source), g in df.groupby(
            ["horizon_days", "model_name", "strategy", "source"], sort=True
        ):
            window_strategy = f"{strategy}__window"
            row = compute_block_metrics(
                g,
                _safe_date(window_end),
                int(horizon_days),
                str(model_name),
                window_strategy,
                label_basis_for_model(str(model_name)),
                str(source),
            )
            if row is not None:
                results_rows.append(row)

        upsert_metrics(conn, results_rows)
        logger.info("Stored %d metric rows into model_eval_metrics_daily.", len(results_rows))

    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        logger.exception("Model evaluation failed.")
        raise
    finally:
        conn.close()


# -----------------------------------------------------------------------------
# DAGs (two schedules: 8:30 AM ET + 5:00 PM ET)
# -----------------------------------------------------------------------------
default_args = {"owner": "airflow", "retries": 1, "retry_delay": timedelta(minutes=5)}

with DAG(
    dag_id="eod_xgb_eval_metrics_daily_morning",
    start_date=pendulum.datetime(2025, 12, 20, tz=LOCAL_TZ),
    schedule="15 9 * * 1-5",  # after the 8:30 AM production prediction run
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["production-monitoring", "ml", "xgboost", "eval", "metrics"],
) as dag_morning:
    PythonOperator(
        task_id="compute_eval_metrics",
        python_callable=compute_and_store_metrics,
    )
