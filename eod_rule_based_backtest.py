from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from datetime import date, timedelta

import pandas as pd
import pendulum
import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator
from psycopg2.extras import execute_values


logger = logging.getLogger("airflow.task")
LOCAL_TZ = pendulum.timezone("America/New_York")

DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "postgres"),
    "port": int(os.getenv("POSTGRES_PORT", "5432")),
    "dbname": os.getenv("POSTGRES_DB", "stocks"),
    "user": os.getenv("POSTGRES_USER", "postgres"),
    "password": os.getenv("POSTGRES_PASSWORD", "postgres"),
}

RUN_NAME = os.getenv("RULE_BACKTEST_RUN_NAME", "rule_exit_breakout_v1")
MODEL_FAMILY = os.getenv("RULE_BACKTEST_MODEL_FAMILY", "xgb_forward_v2_backtest")
XGB_ENTRY_STRATEGY = os.getenv("RULE_BACKTEST_ENTRY_STRATEGY", "forward_return_top10_buy")
HORIZONS = tuple(int(x) for x in os.getenv("RULE_BACKTEST_HORIZONS", "7,20").split(","))
LEVEL_WINDOW = int(os.getenv("RULE_BACKTEST_LEVEL_WINDOW", "20"))
STOP_LOSS_PCT = float(os.getenv("RULE_BACKTEST_STOP_LOSS_PCT", "0.05"))
TRAILING_STOP_PCT = float(os.getenv("RULE_BACKTEST_TRAILING_STOP_PCT", "0.08"))
CONFIRM_VOLUME_RATIO = float(os.getenv("RULE_BACKTEST_CONFIRM_VOLUME_RATIO", "1.20"))
COST_BPS_PER_SIDE = float(os.getenv("RULE_BACKTEST_COST_BPS_PER_SIDE", "10"))

EXIT_POLICIES = (
    "fixed_horizon",
    "support_breakdown",
    "confirmed_breakdown",
    "stop_loss",
    "trailing_stop",
)

RESULT_COLUMNS = (
    "run_name", "entry_signal_date", "ticker", "horizon_days", "entry_rule",
    "exit_policy", "entry_date", "exit_date", "entry_price", "exit_price",
    "gross_return", "transaction_cost", "net_return", "exit_reason", "holding_days",
)


@dataclass(frozen=True)
class Candidate:
    signal_date: date
    ticker: str
    horizon_days: int
    entry_rule: str


def validate_config() -> None:
    if not HORIZONS or any(h <= 0 for h in HORIZONS):
        raise ValueError("RULE_BACKTEST_HORIZONS must contain positive integers")
    if LEVEL_WINDOW not in (20, 60, 120):
        raise ValueError("RULE_BACKTEST_LEVEL_WINDOW must be 20, 60, or 120")
    if not 0 < STOP_LOSS_PCT < 1 or not 0 < TRAILING_STOP_PCT < 1:
        raise ValueError("Stop percentages must be between 0 and 1")
    if COST_BPS_PER_SIDE < 0:
        raise ValueError("Transaction cost cannot be negative")


def load_xgb_candidates(conn) -> list[Candidate]:
    sql = """
        SELECT DISTINCT ON (as_of_date, ticker, horizon_days)
            as_of_date, ticker, horizon_days
        FROM backtest_trade_signals
        WHERE split_part(model_name, '@', 1) = %s
          AND strategy = %s
          AND signal = 'BUY'
          AND horizon_days = ANY(%s)
        ORDER BY as_of_date, ticker, horizon_days, created_at DESC
    """
    with conn.cursor() as cur:
        cur.execute(sql, (MODEL_FAMILY, XGB_ENTRY_STRATEGY, list(HORIZONS)))
        return [Candidate(row[0], row[1], row[2], "xgb_top10_buy") for row in cur.fetchall()]


def load_prices_and_rules(conn, min_date: date, max_date: date) -> pd.DataFrame:
    support_col = f"support_{LEVEL_WINDOW}d"
    resistance_col = f"resistance_{LEVEL_WINDOW}d"
    breakdown_col = f"breakdown_{LEVEL_WINDOW}d"
    breakout_col = f"breakout_{LEVEL_WINDOW}d"
    sql = f"""
        SELECT
            f.trade_date,
            f.ticker,
            d.open::float8 AS open_price,
            d.high::float8 AS high_price,
            d.low::float8 AS low_price,
            d.close::float8 AS close_price,
            f.{support_col}::float8 AS support_price,
            f.{resistance_col}::float8 AS resistance_price,
            f.{breakdown_col} AS breakdown,
            f.{breakout_col} AS breakout,
            f.relative_volume_20::float8 AS relative_volume_20,
            f.macd_hist::float8 AS macd_hist
        FROM daily_features_240d f
        JOIN daily_market_summary d
          ON d.trade_date = f.trade_date
         AND d.ticker = f.ticker
        JOIN tickers_info t
          ON t.ticker = f.ticker
        WHERE f.trade_date BETWEEN %s AND %s
          AND (t.sp500 = TRUE OR t.qqq = TRUE)
          AND COALESCE(t.type, '') <> 'ETF'
        ORDER BY f.ticker, f.trade_date
    """
    return pd.read_sql(sql, conn, params=(min_date, max_date))


def technical_candidates(frame: pd.DataFrame) -> list[Candidate]:
    candidates: list[Candidate] = []
    for row in frame.itertuples(index=False):
        is_breakout = pd.notna(row.breakout) and bool(row.breakout)
        if is_breakout:
            for horizon in HORIZONS:
                candidates.append(Candidate(row.trade_date, row.ticker, horizon, f"resistance_breakout_{LEVEL_WINDOW}d"))
        confirmed = (
            is_breakout
            and pd.notna(row.relative_volume_20)
            and row.relative_volume_20 >= CONFIRM_VOLUME_RATIO
            and pd.notna(row.macd_hist)
            and row.macd_hist > 0
        )
        if confirmed:
            for horizon in HORIZONS:
                candidates.append(Candidate(row.trade_date, row.ticker, horizon, f"confirmed_resistance_breakout_{LEVEL_WINDOW}d"))
    return candidates


def exit_at_open(row: pd.Series) -> float | None:
    if pd.notna(row["open_price"]) and row["open_price"] > 0:
        return float(row["open_price"])
    if pd.notna(row["close_price"]) and row["close_price"] > 0:
        return float(row["close_price"])
    return None


def simulate_trade(candidate: Candidate, prices: pd.DataFrame, policy: str) -> dict | None:
    signal_matches = prices.index[prices["trade_date"] == candidate.signal_date]
    if len(signal_matches) == 0:
        return None
    signal_idx = int(signal_matches[0])
    entry_idx = signal_idx + 1
    fixed_exit_idx = signal_idx + candidate.horizon_days
    if entry_idx >= len(prices) or fixed_exit_idx >= len(prices):
        return None

    entry_price = exit_at_open(prices.iloc[entry_idx])
    if entry_price is None:
        return None

    exit_idx = fixed_exit_idx
    exit_price = float(prices.iloc[fixed_exit_idx]["close_price"])
    exit_reason = "fixed_horizon"

    if policy in ("support_breakdown", "confirmed_breakdown"):
        for idx in range(entry_idx, fixed_exit_idx):
            row = prices.iloc[idx]
            triggered = bool(row["breakdown"]) if pd.notna(row["breakdown"]) else False
            if policy == "confirmed_breakdown":
                triggered = (
                    triggered
                    and pd.notna(row["relative_volume_20"])
                    and row["relative_volume_20"] >= CONFIRM_VOLUME_RATIO
                    and pd.notna(row["macd_hist"])
                    and row["macd_hist"] < 0
                )
            if triggered and idx + 1 <= fixed_exit_idx:
                candidate_exit = exit_at_open(prices.iloc[idx + 1])
                if candidate_exit is not None:
                    exit_idx = idx + 1
                    exit_price = candidate_exit
                    exit_reason = policy
                    break

    elif policy == "stop_loss":
        stop_price = entry_price * (1 - STOP_LOSS_PCT)
        for idx in range(entry_idx, fixed_exit_idx + 1):
            row = prices.iloc[idx]
            if pd.notna(row["low_price"]) and row["low_price"] <= stop_price:
                day_open = exit_at_open(row)
                exit_idx = idx
                exit_price = min(day_open, stop_price) if day_open is not None else stop_price
                exit_reason = "stop_loss"
                break

    elif policy == "trailing_stop":
        peak = entry_price
        for idx in range(entry_idx, fixed_exit_idx + 1):
            row = prices.iloc[idx]
            trailing_price = peak * (1 - TRAILING_STOP_PCT)
            if pd.notna(row["low_price"]) and row["low_price"] <= trailing_price:
                day_open = exit_at_open(row)
                exit_idx = idx
                exit_price = min(day_open, trailing_price) if day_open is not None else trailing_price
                exit_reason = "trailing_stop"
                break
            if pd.notna(row["high_price"]):
                peak = max(peak, float(row["high_price"]))

    if not exit_price or exit_price <= 0:
        return None

    gross_return = exit_price / entry_price - 1
    transaction_cost = 2 * COST_BPS_PER_SIDE / 10_000
    return {
        "run_name": RUN_NAME,
        "entry_signal_date": candidate.signal_date,
        "ticker": candidate.ticker,
        "horizon_days": candidate.horizon_days,
        "entry_rule": candidate.entry_rule,
        "exit_policy": policy,
        "entry_date": prices.iloc[entry_idx]["trade_date"],
        "exit_date": prices.iloc[exit_idx]["trade_date"],
        "entry_price": entry_price,
        "exit_price": exit_price,
        "gross_return": gross_return,
        "transaction_cost": transaction_cost,
        "net_return": gross_return - transaction_cost,
        "exit_reason": exit_reason,
        "holding_days": exit_idx - entry_idx + 1,
    }


def simulate_candidates(frame: pd.DataFrame, candidates: list[Candidate]) -> list[dict]:
    prices_by_ticker = {
        ticker: group.sort_values("trade_date").reset_index(drop=True)
        for ticker, group in frame.groupby("ticker", sort=False)
    }
    results: list[dict] = []
    # Select a common, non-overlapping entry cohort using the fixed-horizon
    # holding period. Every exit policy must then be evaluated on exactly those
    # entries; otherwise faster exits admit extra trades and bias comparisons.
    busy_until: dict[tuple[str, int, str], date] = {}

    for candidate in sorted(candidates, key=lambda x: (x.signal_date, x.ticker, x.horizon_days, x.entry_rule)):
        prices = prices_by_ticker.get(candidate.ticker)
        if prices is None:
            continue
        key = (candidate.ticker, candidate.horizon_days, candidate.entry_rule)
        if candidate.signal_date <= busy_until.get(key, date.min):
            continue

        fixed_result = simulate_trade(candidate, prices, "fixed_horizon")
        if not fixed_result:
            continue
        results.append(fixed_result)
        busy_until[key] = fixed_result["exit_date"]

        for policy in EXIT_POLICIES:
            if policy == "fixed_horizon":
                continue
            result = simulate_trade(candidate, prices, policy)
            if result:
                results.append(result)
    return results


def replace_results(conn, results: list[dict]) -> None:
    insert_sql = f"""
        INSERT INTO rule_based_backtest_results ({', '.join(RESULT_COLUMNS)})
        VALUES %s
    """
    with conn.cursor() as cur:
        cur.execute("DELETE FROM rule_based_backtest_results WHERE run_name = %s", (RUN_NAME,))
        if results:
            values = [tuple(row[column] for column in RESULT_COLUMNS) for row in results]
            execute_values(cur, insert_sql, values, page_size=2000)
    conn.commit()


def run_rule_based_backtest(**context) -> None:
    validate_config()
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        xgb_candidates = load_xgb_candidates(conn)
        if not xgb_candidates:
            raise ValueError("No clean XGBoost top-10 BUY backtest signals found")

        conf = (context.get("dag_run").conf or {}) if context.get("dag_run") else {}
        first_signal = min(c.signal_date for c in xgb_candidates)
        last_signal = max(c.signal_date for c in xgb_candidates)
        start_date = pendulum.parse(conf["start_date"]).date() if conf.get("start_date") else first_signal
        end_date = pendulum.parse(conf["end_date"]).date() if conf.get("end_date") else last_signal
        if start_date > end_date:
            raise ValueError("start_date cannot be after end_date")

        xgb_candidates = [c for c in xgb_candidates if start_date <= c.signal_date <= end_date]
        frame = load_prices_and_rules(
            conn,
            start_date - timedelta(days=5),
            end_date + timedelta(days=max(HORIZONS) * 2),
        )
        if frame.empty:
            raise ValueError("No feature/price rows found for the requested backtest period")

        candidates = xgb_candidates + [
            c for c in technical_candidates(frame)
            if start_date <= c.signal_date <= end_date
        ]
        results = simulate_candidates(frame, candidates)
        if not results:
            raise ValueError("No completed rule-based trades were produced")

        replace_results(conn, results)
        logger.info(
            "Rule backtest complete run=%s candidates=%d trades=%d range=%s..%s",
            RUN_NAME, len(candidates), len(results), start_date, end_date,
        )
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="eod_rule_based_exit_breakout_backtest",
    default_args=default_args,
    start_date=pendulum.datetime(2026, 1, 1, tz=LOCAL_TZ),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["research", "backtest", "exit", "breakout"],
) as dag:
    PythonOperator(
        task_id="run_rule_based_backtest",
        python_callable=run_rule_based_backtest,
    )
