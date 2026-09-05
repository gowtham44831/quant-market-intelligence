from __future__ import annotations

import json
import logging
import math
import os
from dataclasses import dataclass
from datetime import date, timedelta

import pandas as pd
import pendulum
import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator
from psycopg2.extras import Json, execute_values


logger = logging.getLogger("airflow.task")
LOCAL_TZ = pendulum.timezone("America/New_York")

DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "postgres"),
    "port": int(os.getenv("POSTGRES_PORT", "5432")),
    "dbname": os.getenv("POSTGRES_DB", "stocks"),
    "user": os.getenv("POSTGRES_USER", "postgres"),
    "password": os.getenv("POSTGRES_PASSWORD", "postgres"),
}

INITIAL_CAPITAL = 5000.0
MAX_POSITIONS = 10
MAX_POSITION_PCT = 0.10
HORIZON_DAYS = 7
COST_BPS_PER_SIDE = 10.0
SLIPPAGE_BPS_PER_SIDE = 5.0
ANNUAL_BORROW_BPS = float(os.getenv("SHORT_BACKTEST_ANNUAL_BORROW_BPS", "300"))
STRATEGY = "forward_return_top10_sell"

SHORT_VARIANTS = (
    {
        "run_name": "portfolio_short_xgb_forward_v2_7d_5k_whole",
        "model_family": "xgb_forward_v2_backtest",
    },
    {
        "run_name": "portfolio_short_xgb_forward_v3_context_7d_5k_whole",
        "model_family": "xgb_forward_v3_context_backtest",
    },
)


@dataclass
class ShortPosition:
    ticker: str
    signal_date: date
    entry_date: date
    score: float | None
    shares: float
    entry_price: float
    entry_value: float
    entry_cost: float
    collateral: float
    last_close: float
    holding_days: int = 0


def load_short_signals(conn, model_family: str) -> pd.DataFrame:
    return pd.read_sql(
        """
        SELECT DISTINCT ON (as_of_date, ticker)
            as_of_date AS signal_date,
            ticker,
            score::float8 AS score
        FROM backtest_trade_signals
        WHERE split_part(model_name, '@', 1) = %s
          AND strategy = %s
          AND horizon_days = %s
          AND signal = 'SELL'
        ORDER BY as_of_date, ticker, created_at DESC
        """,
        conn,
        params=(model_family, STRATEGY, HORIZON_DAYS),
    )


def load_market_data(
    conn, tickers: list[str], start_date: date, end_date: date
) -> pd.DataFrame:
    return pd.read_sql(
        """
        SELECT
            trade_date,
            ticker,
            open::float8 AS open_price,
            close::float8 AS close_price
        FROM daily_market_summary
        WHERE ticker = ANY(%s)
          AND trade_date BETWEEN %s AND %s
          AND open > 0
          AND close > 0
        ORDER BY trade_date, ticker
        """,
        conn,
        params=(tickers, start_date, end_date),
    )


def next_calendar_date(calendar: list[date], value: date) -> date | None:
    for trading_date in calendar:
        if trading_date > value:
            return trading_date
    return None


def benchmark_equity(
    calendar: list[date],
    prices: dict[tuple[date, str], tuple[float, float]],
    ticker: str,
) -> dict[date, float | None]:
    first_close = next(
        (prices[(day, ticker)][1] for day in calendar if (day, ticker) in prices),
        None,
    )
    if not first_close:
        return {day: None for day in calendar}
    last_close = first_close
    result = {}
    for day in calendar:
        if (day, ticker) in prices:
            last_close = prices[(day, ticker)][1]
        result[day] = INITIAL_CAPITAL * last_close / first_close
    return result


def simulate_short_portfolio(
    run_name: str, signals: pd.DataFrame, market: pd.DataFrame
) -> tuple[list[dict], list[dict]]:
    calendar = sorted(market.loc[market["ticker"] == "SPY", "trade_date"].unique())
    if len(calendar) <= HORIZON_DAYS:
        raise ValueError("Not enough SPY dates for the short portfolio simulation")

    prices = {
        (row.trade_date, row.ticker): (float(row.open_price), float(row.close_price))
        for row in market.itertuples(index=False)
    }
    complete_signal_dates = {
        day for index, day in enumerate(calendar)
        if index + HORIZON_DAYS < len(calendar)
    }
    entries_by_date: dict[date, list[dict]] = {}
    for row in signals.itertuples(index=False):
        if row.signal_date not in complete_signal_dates:
            continue
        entry_date = next_calendar_date(calendar, row.signal_date)
        if entry_date is None:
            continue
        entries_by_date.setdefault(entry_date, []).append({
            "ticker": row.ticker,
            "signal_date": row.signal_date,
            "score": None if pd.isna(row.score) else float(row.score),
        })
    for candidates in entries_by_date.values():
        candidates.sort(
            key=lambda item: item["score"] if item["score"] is not None else -math.inf,
            reverse=True,
        )

    spy_equity = benchmark_equity(calendar, prices, "SPY")
    qqq_equity = benchmark_equity(calendar, prices, "QQQ")
    cost_rate = COST_BPS_PER_SIDE / 10_000
    slippage_rate = SLIPPAGE_BPS_PER_SIDE / 10_000
    borrow_rate = ANNUAL_BORROW_BPS / 10_000
    cash = INITIAL_CAPITAL
    previous_equity = INITIAL_CAPITAL
    peak_equity = INITIAL_CAPITAL
    positions: dict[str, ShortPosition] = {}
    daily_rows: list[dict] = []
    trade_rows: list[dict] = []

    for trading_date in calendar:
        entries_today = 0
        exits_today = 0

        # SELL signals are observed after the prior close and shorted at today's
        # open with adverse slippage. Cash collateral prevents implicit leverage.
        for candidate in entries_by_date.get(trading_date, []):
            ticker = candidate["ticker"]
            if len(positions) >= MAX_POSITIONS or ticker in positions:
                continue
            price = prices.get((trading_date, ticker))
            if not price:
                continue
            execution_price = price[0] * (1 - slippage_rate)
            target_value = min(
                previous_equity * MAX_POSITION_PCT,
                cash / (1 + cost_rate),
            )
            shares = float(math.floor(target_value / execution_price))
            if shares < 1:
                continue
            entry_value = shares * execution_price
            entry_cost = entry_value * cost_rate
            collateral = entry_value
            if collateral + entry_cost > cash:
                continue
            cash -= collateral + entry_cost
            positions[ticker] = ShortPosition(
                ticker=ticker,
                signal_date=candidate["signal_date"],
                entry_date=trading_date,
                score=candidate["score"],
                shares=shares,
                entry_price=execution_price,
                entry_value=entry_value,
                entry_cost=entry_cost,
                collateral=collateral,
                last_close=price[1],
            )
            entries_today += 1

        for ticker, position in list(positions.items()):
            price = prices.get((trading_date, ticker))
            if not price:
                continue
            position.last_close = price[1]
            position.holding_days += 1
            if position.holding_days < HORIZON_DAYS:
                continue

            cover_price = price[1] * (1 + slippage_rate)
            cover_value = position.shares * cover_price
            exit_cost = cover_value * cost_rate
            borrow_cost = (
                position.entry_value * borrow_rate * position.holding_days / 252
            )
            gross_pnl = position.entry_value - cover_value
            net_pnl = gross_pnl - position.entry_cost - exit_cost - borrow_cost
            cash += position.collateral + gross_pnl - exit_cost - borrow_cost
            initial_outlay = position.collateral + position.entry_cost
            trade_rows.append({
                "run_name": run_name,
                "ticker": ticker,
                "signal_date": position.signal_date,
                "entry_date": position.entry_date,
                "exit_date": trading_date,
                "horizon_days": HORIZON_DAYS,
                "score": position.score,
                "shares": position.shares,
                "entry_price": position.entry_price,
                "exit_price": cover_price,
                "entry_value": position.entry_value,
                "entry_cost": position.entry_cost,
                "exit_value": cover_value,
                "exit_cost": exit_cost + borrow_cost,
                "net_pnl": net_pnl,
                "net_return": net_pnl / initial_outlay,
                "holding_days": position.holding_days,
                "exit_reason": "fixed_short_horizon",
            })
            del positions[ticker]
            exits_today += 1

        position_equity = 0.0
        for position in positions.values():
            accrued_borrow = (
                position.entry_value * borrow_rate * position.holding_days / 252
            )
            position_equity += (
                position.collateral
                + (position.entry_price - position.last_close) * position.shares
                - accrued_borrow
            )
        equity = cash + position_equity
        daily_return = equity / previous_equity - 1 if previous_equity else None
        peak_equity = max(peak_equity, equity)
        drawdown = equity / peak_equity - 1 if peak_equity else 0.0
        daily_rows.append({
            "run_name": run_name,
            "trade_date": trading_date,
            "cash": cash,
            # For a collateralized short, this is reserved collateral plus
            # unrealized P&L net of accrued borrow cost. Keeping it here
            # preserves the daily invariant: cash + market_value = equity.
            "market_value": position_equity,
            "equity": equity,
            "daily_return": daily_return,
            "drawdown": drawdown,
            "open_positions": len(positions),
            "entries": entries_today,
            "exits": exits_today,
            "spy_equity": spy_equity[trading_date],
            "qqq_equity": qqq_equity[trading_date],
        })
        previous_equity = equity

    return daily_rows, trade_rows


def safe_float(value) -> float | None:
    if value is None or pd.isna(value) or math.isinf(float(value)):
        return None
    return float(value)


def build_summary(
    run_name: str,
    model_family: str,
    daily_rows: list[dict],
    trade_rows: list[dict],
) -> dict:
    daily = pd.DataFrame(daily_rows)
    trades = pd.DataFrame(trade_rows)
    first_date = daily.iloc[0]["trade_date"]
    last_date = daily.iloc[-1]["trade_date"]
    final_equity = float(daily.iloc[-1]["equity"])
    total_return = final_equity / INITIAL_CAPITAL - 1
    elapsed_days = max((last_date - first_date).days, 1)
    returns = pd.to_numeric(daily["daily_return"], errors="coerce").dropna()
    volatility = returns.std(ddof=1) * math.sqrt(252) if len(returns) > 1 else None
    sharpe = (
        returns.mean() / returns.std(ddof=1) * math.sqrt(252)
        if len(returns) > 1 and returns.std(ddof=1) > 0 else None
    )
    trade_returns = pd.to_numeric(
        trades.get("net_return", pd.Series(dtype=float)), errors="coerce"
    ).dropna()
    spy_final = safe_float(daily.iloc[-1]["spy_equity"])
    qqq_final = safe_float(daily.iloc[-1]["qqq_equity"])
    spy_return = spy_final / INITIAL_CAPITAL - 1 if spy_final else None
    qqq_return = qqq_final / INITIAL_CAPITAL - 1 if qqq_final else None
    return {
        "run_name": run_name,
        "model_family": model_family,
        "strategy": STRATEGY,
        "horizon_days": HORIZON_DAYS,
        "initial_capital": INITIAL_CAPITAL,
        "final_equity": final_equity,
        "total_return": total_return,
        "cagr": (final_equity / INITIAL_CAPITAL) ** (365.25 / elapsed_days) - 1,
        "annualized_volatility": safe_float(volatility),
        "sharpe_ratio": safe_float(sharpe),
        "max_drawdown": float(daily["drawdown"].min()),
        "average_exposure": safe_float((daily["market_value"] / daily["equity"]).mean()),
        "max_positions_used": int(daily["open_positions"].max()),
        "trades": len(trade_rows),
        "win_rate": safe_float((trade_returns > 0).mean()) if len(trade_returns) else None,
        "average_trade_return": safe_float(trade_returns.mean()) if len(trade_returns) else None,
        "median_trade_return": safe_float(trade_returns.median()) if len(trade_returns) else None,
        "spy_return": spy_return,
        "qqq_return": qqq_return,
        "excess_return_spy": total_return - spy_return if spy_return is not None else None,
        "excess_return_qqq": total_return - qqq_return if qqq_return is not None else None,
        "config_json": {
            "direction": "SHORT",
            "max_positions": MAX_POSITIONS,
            "max_position_pct": MAX_POSITION_PCT,
            "cost_bps_per_side": COST_BPS_PER_SIDE,
            "slippage_bps_per_side": SLIPPAGE_BPS_PER_SIDE,
            "annual_borrow_bps": ANNUAL_BORROW_BPS,
            "entry_execution": "next_session_open",
            "exit_execution": "horizon_session_close",
            "fractional_shares": False,
            "cash_collateral_pct": 1.0,
            "leverage": False,
            "borrow_availability_assumed": True,
        },
        "first_date": first_date,
        "last_date": last_date,
    }


def replace_run(conn, summary: dict, daily_rows: list[dict], trade_rows: list[dict]) -> None:
    run_columns = tuple(summary)
    daily_columns = tuple(daily_rows[0])
    trade_columns = tuple(trade_rows[0]) if trade_rows else ()
    with conn.cursor() as cur:
        cur.execute(
            "DELETE FROM portfolio_backtest_runs WHERE run_name = %s",
            (summary["run_name"],),
        )
        cur.execute(
            f"INSERT INTO portfolio_backtest_runs ({', '.join(run_columns)}) "
            f"VALUES ({', '.join(['%s'] * len(run_columns))})",
            [
                Json(summary[column]) if column == "config_json" else summary[column]
                for column in run_columns
            ],
        )
        execute_values(
            cur,
            f"INSERT INTO portfolio_backtest_daily ({', '.join(daily_columns)}) VALUES %s",
            [tuple(row[column] for column in daily_columns) for row in daily_rows],
            page_size=1000,
        )
        if trade_rows:
            execute_values(
                cur,
                f"INSERT INTO portfolio_backtest_trades ({', '.join(trade_columns)}) VALUES %s",
                [tuple(row[column] for column in trade_columns) for row in trade_rows],
                page_size=1000,
            )
    conn.commit()


def run_short_portfolio_backtest(**context) -> None:
    if ANNUAL_BORROW_BPS < 0:
        raise ValueError("SHORT_BACKTEST_ANNUAL_BORROW_BPS cannot be negative")
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        loaded = {
            variant["model_family"]: load_short_signals(conn, variant["model_family"])
            for variant in SHORT_VARIANTS
        }
        for frame in loaded.values():
            if frame.empty:
                raise ValueError("A baseline or challenger SELL signal set is empty")
            frame["signal_date"] = pd.to_datetime(frame["signal_date"]).dt.date

        conf = (context.get("dag_run").conf or {}) if context.get("dag_run") else {}
        common_start = max(frame["signal_date"].min() for frame in loaded.values())
        common_end = min(frame["signal_date"].max() for frame in loaded.values())
        start_date = pendulum.parse(conf["start_date"]).date() if conf.get("start_date") else common_start
        end_date = pendulum.parse(conf["end_date"]).date() if conf.get("end_date") else common_end

        all_tickers = {"SPY", "QQQ"}
        for frame in loaded.values():
            all_tickers.update(
                frame.loc[
                    (frame["signal_date"] >= start_date)
                    & (frame["signal_date"] <= end_date),
                    "ticker",
                ].tolist()
            )
        market = load_market_data(
            conn,
            sorted(all_tickers),
            start_date,
            end_date + timedelta(days=HORIZON_DAYS * 3),
        )
        market["trade_date"] = pd.to_datetime(market["trade_date"]).dt.date

        for variant in SHORT_VARIANTS:
            signals = loaded[variant["model_family"]]
            signals = signals[
                (signals["signal_date"] >= start_date)
                & (signals["signal_date"] <= end_date)
            ].copy()
            daily_rows, trade_rows = simulate_short_portfolio(
                variant["run_name"], signals, market
            )
            summary = build_summary(
                variant["run_name"],
                variant["model_family"],
                daily_rows,
                trade_rows,
            )
            replace_run(conn, summary, daily_rows, trade_rows)
            logger.info("SHORT backtest complete: %s", json.dumps(summary, default=str))
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
    dag_id="eod_xgb_short_portfolio_backtest",
    default_args=default_args,
    start_date=pendulum.datetime(2026, 1, 1, tz=LOCAL_TZ),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["research", "portfolio", "backtest", "xgboost", "short"],
) as dag:
    PythonOperator(
        task_id="run_short_portfolio_backtest",
        python_callable=run_short_portfolio_backtest,
    )
