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

RUN_NAME = os.getenv("PORTFOLIO_BACKTEST_RUN_NAME", "portfolio_xgb_forward_v2_5k_whole")
MODEL_FAMILY = os.getenv("PORTFOLIO_BACKTEST_MODEL_FAMILY", "xgb_forward_v2_backtest")
STRATEGY = os.getenv("PORTFOLIO_BACKTEST_STRATEGY", "forward_return_top10_buy")
HORIZON_DAYS = int(os.getenv("PORTFOLIO_BACKTEST_HORIZON_DAYS", "20"))
INITIAL_CAPITAL = float(os.getenv("PORTFOLIO_BACKTEST_INITIAL_CAPITAL", "5000"))
MAX_POSITIONS = int(os.getenv("PORTFOLIO_BACKTEST_MAX_POSITIONS", "10"))
MAX_POSITION_PCT = float(os.getenv("PORTFOLIO_BACKTEST_MAX_POSITION_PCT", "0.10"))
COST_BPS_PER_SIDE = float(os.getenv("PORTFOLIO_BACKTEST_COST_BPS_PER_SIDE", "10"))
SLIPPAGE_BPS_PER_SIDE = float(os.getenv("PORTFOLIO_BACKTEST_SLIPPAGE_BPS_PER_SIDE", "5"))
ALLOW_FRACTIONAL_SHARES = os.getenv(
    "PORTFOLIO_BACKTEST_FRACTIONAL_SHARES", "false"
).lower() == "true"
_DOWN_ENTRY_BLOCK_RAW = os.getenv("PORTFOLIO_BACKTEST_DOWN_ENTRY_BLOCK", "").strip()
_DOWN_EXIT_RAW = os.getenv("PORTFOLIO_BACKTEST_DOWN_EXIT", "").strip()
DOWN_ENTRY_BLOCK_THRESHOLD = float(_DOWN_ENTRY_BLOCK_RAW) if _DOWN_ENTRY_BLOCK_RAW else None
DOWN_EXIT_THRESHOLD = float(_DOWN_EXIT_RAW) if _DOWN_EXIT_RAW else None


@dataclass
class Position:
    ticker: str
    signal_date: date
    entry_date: date
    score: float | None
    shares: float
    entry_price: float
    entry_value: float
    entry_cost: float
    last_close: float
    holding_days: int = 0


def validate_config() -> None:
    if HORIZON_DAYS <= 0:
        raise ValueError("PORTFOLIO_BACKTEST_HORIZON_DAYS must be positive")
    if INITIAL_CAPITAL <= 0 or MAX_POSITIONS <= 0:
        raise ValueError("Capital and maximum positions must be positive")
    if not 0 < MAX_POSITION_PCT <= 1:
        raise ValueError("PORTFOLIO_BACKTEST_MAX_POSITION_PCT must be in (0, 1]")
    if MAX_POSITIONS * MAX_POSITION_PCT > 1.000001:
        raise ValueError("Maximum positions multiplied by position percentage cannot exceed 100% without leverage")
    if COST_BPS_PER_SIDE < 0 or SLIPPAGE_BPS_PER_SIDE < 0:
        raise ValueError("Costs and slippage cannot be negative")
    for name, threshold in (
        ("PORTFOLIO_BACKTEST_DOWN_ENTRY_BLOCK", DOWN_ENTRY_BLOCK_THRESHOLD),
        ("PORTFOLIO_BACKTEST_DOWN_EXIT", DOWN_EXIT_THRESHOLD),
    ):
        if threshold is not None and not 0 <= threshold <= 1:
            raise ValueError(f"{name} must be between 0 and 1")


def load_signals(conn) -> pd.DataFrame:
    sql = """
        SELECT DISTINCT ON (s.as_of_date, s.ticker)
            s.as_of_date AS signal_date,
            s.ticker,
            s.score::float8 AS score,
            p.prob_down::float8 AS prob_down
        FROM backtest_trade_signals s
        LEFT JOIN model_predictions p
          ON p.as_of_date = s.as_of_date
         AND p.ticker = s.ticker
         AND p.horizon_days = s.horizon_days
         AND p.model_name = s.model_name
        WHERE split_part(s.model_name, '@', 1) = %s
          AND s.strategy = %s
          AND s.horizon_days = %s
          AND s.signal = 'BUY'
        ORDER BY s.as_of_date, s.ticker, s.created_at DESC
    """
    return pd.read_sql(sql, conn, params=(MODEL_FAMILY, STRATEGY, HORIZON_DAYS))


def load_down_predictions(
    conn, tickers: list[str], start_date: date, end_date: date
) -> pd.DataFrame:
    sql = """
        SELECT DISTINCT ON (as_of_date, ticker)
            as_of_date AS signal_date,
            ticker,
            prob_down::float8 AS prob_down
        FROM model_predictions
        WHERE split_part(model_name, '@', 1) = %s
          AND horizon_days = %s
          AND as_of_date BETWEEN %s AND %s
          AND ticker = ANY(%s)
          AND prob_down IS NOT NULL
        ORDER BY as_of_date, ticker, created_at DESC
    """
    return pd.read_sql(
        sql,
        conn,
        params=(MODEL_FAMILY, HORIZON_DAYS, start_date, end_date, tickers),
    )


def load_market_data(conn, tickers: list[str], start_date: date, end_date: date) -> pd.DataFrame:
    sql = """
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
    """
    return pd.read_sql(sql, conn, params=(tickers, start_date, end_date))


def next_calendar_date(calendar: list[date], value: date) -> date | None:
    for trading_date in calendar:
        if trading_date > value:
            return trading_date
    return None


def completed_signal_dates(calendar: list[date]) -> set[date]:
    index_by_date = {value: idx for idx, value in enumerate(calendar)}
    return {
        value for value, idx in index_by_date.items()
        if idx + HORIZON_DAYS < len(calendar)
    }


def calculate_benchmark_equity(
    calendar: list[date],
    price_lookup: dict[tuple[date, str], tuple[float, float]],
    ticker: str,
) -> dict[date, float | None]:
    first_close = next(
        (price_lookup[(d, ticker)][1] for d in calendar if (d, ticker) in price_lookup),
        None,
    )
    if not first_close:
        return {d: None for d in calendar}
    last_close = first_close
    result = {}
    for trading_date in calendar:
        price = price_lookup.get((trading_date, ticker))
        if price:
            last_close = price[1]
        result[trading_date] = INITIAL_CAPITAL * last_close / first_close
    return result


def simulate_portfolio(
    signals: pd.DataFrame,
    market: pd.DataFrame,
    down_predictions: pd.DataFrame | None = None,
) -> tuple[list[dict], list[dict]]:
    spy_rows = market[market["ticker"] == "SPY"]
    calendar = sorted(spy_rows["trade_date"].tolist())
    if len(calendar) <= HORIZON_DAYS:
        raise ValueError("Not enough SPY trading dates to simulate the requested horizon")

    price_lookup = {
        (row.trade_date, row.ticker): (float(row.open_price), float(row.close_price))
        for row in market.itertuples(index=False)
    }
    allowed_signal_dates = completed_signal_dates(calendar)
    calendar_set = set(calendar)

    entries_by_date: dict[date, list[dict]] = {}
    for row in signals.itertuples(index=False):
        if row.signal_date not in allowed_signal_dates:
            continue
        entry_date = next_calendar_date(calendar, row.signal_date)
        if entry_date and entry_date in calendar_set:
            entries_by_date.setdefault(entry_date, []).append({
                "ticker": row.ticker,
                "signal_date": row.signal_date,
                "score": None if pd.isna(row.score) else float(row.score),
                "prob_down": None if pd.isna(row.prob_down) else float(row.prob_down),
            })
    for candidates in entries_by_date.values():
        candidates.sort(key=lambda row: row["score"] if row["score"] is not None else -math.inf, reverse=True)

    # A downside prediction observed after a session's close may only trigger an
    # exit at the following session's open. This prevents same-close lookahead.
    risk_exits_by_date: dict[date, dict[str, float]] = {}
    if DOWN_EXIT_THRESHOLD is not None and down_predictions is not None:
        for row in down_predictions.itertuples(index=False):
            execution_date = next_calendar_date(calendar, row.signal_date)
            if execution_date is None or pd.isna(row.prob_down):
                continue
            ticker_risks = risk_exits_by_date.setdefault(execution_date, {})
            ticker_risks[row.ticker] = max(
                ticker_risks.get(row.ticker, -math.inf),
                float(row.prob_down),
            )

    spy_equity = calculate_benchmark_equity(calendar, price_lookup, "SPY")
    qqq_equity = calculate_benchmark_equity(calendar, price_lookup, "QQQ")

    cash = INITIAL_CAPITAL
    positions: dict[str, Position] = {}
    daily_rows: list[dict] = []
    trade_rows: list[dict] = []
    previous_equity = INITIAL_CAPITAL
    peak_equity = INITIAL_CAPITAL
    cost_rate = COST_BPS_PER_SIDE / 10_000
    slippage_rate = SLIPPAGE_BPS_PER_SIDE / 10_000

    for trading_date in calendar:
        entries_today = 0
        exits_today = 0

        # Execute yesterday's downside-risk decisions at today's open before
        # considering new entries derived from the same prior close.
        for ticker, prob_down in risk_exits_by_date.get(trading_date, {}).items():
            if prob_down < DOWN_EXIT_THRESHOLD or ticker not in positions:
                continue
            price = price_lookup.get((trading_date, ticker))
            if not price:
                continue
            position = positions[ticker]
            execution_price = price[0] * (1 - slippage_rate)
            exit_value = position.shares * execution_price
            exit_cost = exit_value * cost_rate
            net_proceeds = exit_value - exit_cost
            initial_outlay = position.entry_value + position.entry_cost
            net_pnl = net_proceeds - initial_outlay
            cash += net_proceeds
            trade_rows.append({
                "run_name": RUN_NAME,
                "ticker": ticker,
                "signal_date": position.signal_date,
                "entry_date": position.entry_date,
                "exit_date": trading_date,
                "horizon_days": HORIZON_DAYS,
                "score": position.score,
                "shares": position.shares,
                "entry_price": position.entry_price,
                "exit_price": execution_price,
                "entry_value": position.entry_value,
                "entry_cost": position.entry_cost,
                "exit_value": exit_value,
                "exit_cost": exit_cost,
                "net_pnl": net_pnl,
                "net_return": net_pnl / initial_outlay,
                "holding_days": position.holding_days,
                "exit_reason": "down_risk_next_open",
            })
            del positions[ticker]
            exits_today += 1

        # Signals are known after the prior close and execute at today's open.
        for candidate in entries_by_date.get(trading_date, []):
            ticker = candidate["ticker"]
            if len(positions) >= MAX_POSITIONS or ticker in positions:
                continue
            if (
                DOWN_ENTRY_BLOCK_THRESHOLD is not None
                and candidate["prob_down"] is not None
                and candidate["prob_down"] >= DOWN_ENTRY_BLOCK_THRESHOLD
            ):
                continue
            price = price_lookup.get((trading_date, ticker))
            if not price:
                continue
            open_price, close_price = price
            execution_price = open_price * (1 + slippage_rate)
            target_value = min(previous_equity * MAX_POSITION_PCT, cash / (1 + cost_rate))
            if target_value <= 0:
                continue
            if ALLOW_FRACTIONAL_SHARES:
                shares = target_value / execution_price
            else:
                shares = float(math.floor(target_value / execution_price))
            if shares < 1 and not ALLOW_FRACTIONAL_SHARES:
                continue

            entry_value = shares * execution_price
            entry_cost = entry_value * cost_rate
            if entry_value + entry_cost > cash:
                continue
            cash -= entry_value + entry_cost
            positions[ticker] = Position(
                ticker=ticker,
                signal_date=candidate["signal_date"],
                entry_date=trading_date,
                score=candidate["score"],
                shares=shares,
                entry_price=execution_price,
                entry_value=entry_value,
                entry_cost=entry_cost,
                last_close=close_price,
            )
            entries_today += 1

        # Mark positions and close at the horizon day's close.
        for ticker, position in list(positions.items()):
            price = price_lookup.get((trading_date, ticker))
            if not price:
                continue
            position.last_close = price[1]
            position.holding_days += 1
            if position.holding_days < HORIZON_DAYS:
                continue

            execution_price = price[1] * (1 - slippage_rate)
            exit_value = position.shares * execution_price
            exit_cost = exit_value * cost_rate
            net_proceeds = exit_value - exit_cost
            initial_outlay = position.entry_value + position.entry_cost
            net_pnl = net_proceeds - initial_outlay
            cash += net_proceeds
            trade_rows.append({
                "run_name": RUN_NAME,
                "ticker": ticker,
                "signal_date": position.signal_date,
                "entry_date": position.entry_date,
                "exit_date": trading_date,
                "horizon_days": HORIZON_DAYS,
                "score": position.score,
                "shares": position.shares,
                "entry_price": position.entry_price,
                "exit_price": execution_price,
                "entry_value": position.entry_value,
                "entry_cost": position.entry_cost,
                "exit_value": exit_value,
                "exit_cost": exit_cost,
                "net_pnl": net_pnl,
                "net_return": net_pnl / initial_outlay,
                "holding_days": position.holding_days,
                "exit_reason": "fixed_horizon",
            })
            del positions[ticker]
            exits_today += 1

        market_value = sum(position.shares * position.last_close for position in positions.values())
        equity = cash + market_value
        daily_return = equity / previous_equity - 1 if previous_equity else None
        peak_equity = max(peak_equity, equity)
        drawdown = equity / peak_equity - 1 if peak_equity else 0.0
        daily_rows.append({
            "run_name": RUN_NAME,
            "trade_date": trading_date,
            "cash": cash,
            "market_value": market_value,
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

    if positions:
        logger.warning("Simulation ended with %d open positions; late incomplete signals were excluded where possible", len(positions))
    return daily_rows, trade_rows


def safe_float(value) -> float | None:
    if value is None or pd.isna(value) or math.isinf(float(value)):
        return None
    return float(value)


def build_run_summary(daily_rows: list[dict], trade_rows: list[dict]) -> dict:
    daily = pd.DataFrame(daily_rows)
    trades = pd.DataFrame(trade_rows)
    if daily.empty:
        raise ValueError("Portfolio simulation produced no daily rows")

    first_date = daily.iloc[0]["trade_date"]
    last_date = daily.iloc[-1]["trade_date"]
    final_equity = float(daily.iloc[-1]["equity"])
    total_return = final_equity / INITIAL_CAPITAL - 1
    elapsed_days = max((last_date - first_date).days, 1)
    cagr = (final_equity / INITIAL_CAPITAL) ** (365.25 / elapsed_days) - 1
    returns = pd.to_numeric(daily["daily_return"], errors="coerce").dropna()
    volatility = returns.std(ddof=1) * math.sqrt(252) if len(returns) > 1 else None
    sharpe = returns.mean() / returns.std(ddof=1) * math.sqrt(252) if len(returns) > 1 and returns.std(ddof=1) > 0 else None
    spy_final = safe_float(daily.iloc[-1]["spy_equity"])
    qqq_final = safe_float(daily.iloc[-1]["qqq_equity"])
    spy_return = spy_final / INITIAL_CAPITAL - 1 if spy_final else None
    qqq_return = qqq_final / INITIAL_CAPITAL - 1 if qqq_final else None

    trade_returns = pd.to_numeric(trades.get("net_return", pd.Series(dtype=float)), errors="coerce").dropna()
    return {
        "run_name": RUN_NAME,
        "model_family": MODEL_FAMILY,
        "strategy": STRATEGY,
        "horizon_days": HORIZON_DAYS,
        "initial_capital": INITIAL_CAPITAL,
        "final_equity": final_equity,
        "total_return": total_return,
        "cagr": cagr,
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
            "max_positions": MAX_POSITIONS,
            "max_position_pct": MAX_POSITION_PCT,
            "cost_bps_per_side": COST_BPS_PER_SIDE,
            "slippage_bps_per_side": SLIPPAGE_BPS_PER_SIDE,
            "entry_execution": "next_session_open",
            "exit_execution": "horizon_session_close",
            "fractional_shares": ALLOW_FRACTIONAL_SHARES,
            "down_entry_block_threshold": DOWN_ENTRY_BLOCK_THRESHOLD,
            "down_exit_threshold": DOWN_EXIT_THRESHOLD,
            "down_exit_execution": "next_session_open" if DOWN_EXIT_THRESHOLD is not None else None,
            "leverage": False,
        },
        "first_date": first_date,
        "last_date": last_date,
    }


def replace_run(conn, summary: dict, daily_rows: list[dict], trade_rows: list[dict]) -> None:
    run_columns = tuple(summary.keys())
    daily_columns = tuple(daily_rows[0].keys())
    trade_columns = tuple(trade_rows[0].keys()) if trade_rows else ()
    run_values = [Json(summary[column]) if column == "config_json" else summary[column] for column in run_columns]

    with conn.cursor() as cur:
        cur.execute("DELETE FROM portfolio_backtest_runs WHERE run_name = %s", (RUN_NAME,))
        cur.execute(
            f"INSERT INTO portfolio_backtest_runs ({', '.join(run_columns)}) VALUES ({', '.join(['%s'] * len(run_columns))})",
            run_values,
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


def run_portfolio_backtest(**context) -> None:
    validate_config()
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        signals = load_signals(conn)
        if signals.empty:
            raise ValueError("No matching clean backtest BUY signals found")
        signals["signal_date"] = pd.to_datetime(signals["signal_date"]).dt.date

        conf = (context.get("dag_run").conf or {}) if context.get("dag_run") else {}
        start_date = pendulum.parse(conf["start_date"]).date() if conf.get("start_date") else signals["signal_date"].min()
        end_date = pendulum.parse(conf["end_date"]).date() if conf.get("end_date") else signals["signal_date"].max()
        signals = signals[(signals["signal_date"] >= start_date) & (signals["signal_date"] <= end_date)].copy()

        tickers = sorted(set(signals["ticker"].tolist()) | {"SPY", "QQQ"})
        market = load_market_data(conn, tickers, start_date, end_date + timedelta(days=HORIZON_DAYS * 3))
        if market.empty:
            raise ValueError("No market data found for portfolio simulation")
        market["trade_date"] = pd.to_datetime(market["trade_date"]).dt.date

        down_predictions = None
        if DOWN_EXIT_THRESHOLD is not None:
            down_predictions = load_down_predictions(
                conn, tickers, start_date, end_date
            )
            down_predictions["signal_date"] = pd.to_datetime(
                down_predictions["signal_date"]
            ).dt.date

        daily_rows, trade_rows = simulate_portfolio(signals, market, down_predictions)
        summary = build_run_summary(daily_rows, trade_rows)
        replace_run(conn, summary, daily_rows, trade_rows)
        logger.info("Portfolio backtest complete: %s", json.dumps(summary, default=str, sort_keys=True))
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


SIZING_VARIANTS = (
    {
        "run_name": "portfolio_xgb_forward_v2_5k_5pos_whole",
        "initial_capital": 5000.0,
        "max_positions": 5,
        "max_position_pct": 0.20,
    },
    {
        "run_name": "portfolio_xgb_forward_v2_5k_8pos_whole",
        "initial_capital": 5000.0,
        "max_positions": 8,
        "max_position_pct": 0.125,
    },
)


def run_sizing_comparison(**context) -> None:
    """Run realistic whole-share sizing alternatives with identical signals and costs."""
    global RUN_NAME, INITIAL_CAPITAL, MAX_POSITIONS, MAX_POSITION_PCT, ALLOW_FRACTIONAL_SHARES

    original_config = (
        RUN_NAME,
        INITIAL_CAPITAL,
        MAX_POSITIONS,
        MAX_POSITION_PCT,
        ALLOW_FRACTIONAL_SHARES,
    )
    try:
        for variant in SIZING_VARIANTS:
            RUN_NAME = variant["run_name"]
            INITIAL_CAPITAL = variant["initial_capital"]
            MAX_POSITIONS = variant["max_positions"]
            MAX_POSITION_PCT = variant["max_position_pct"]
            ALLOW_FRACTIONAL_SHARES = False
            logger.info(
                "Running sizing variant name=%s capital=%.2f positions=%d position_pct=%.3f",
                RUN_NAME,
                INITIAL_CAPITAL,
                MAX_POSITIONS,
                MAX_POSITION_PCT,
            )
            run_portfolio_backtest(**context)
    finally:
        (
            RUN_NAME,
            INITIAL_CAPITAL,
            MAX_POSITIONS,
            MAX_POSITION_PCT,
            ALLOW_FRACTIONAL_SHARES,
        ) = original_config


CHALLENGER_COMPARISON_VARIANTS = (
    {
        "run_name": "portfolio_xgb_forward_v2_7d_5k_whole",
        "model_family": "xgb_forward_v2_backtest",
    },
    {
        "run_name": "portfolio_xgb_forward_v3_context_7d_5k_whole",
        "model_family": "xgb_forward_v3_context_backtest",
    },
)

ROBUSTNESS_PORTFOLIO_VARIANTS = (
    {
        "run_name": "portfolio_xgb_forward_v2_7d_480_5k_whole",
        "model_family": "xgb_forward_v2_7d_480_backtest",
    },
    {
        "run_name": "portfolio_xgb_forward_v3_context_7d_480_5k_whole",
        "model_family": "xgb_forward_v3_context_7d_480_backtest",
    },
)


def run_challenger_portfolio_comparison(**context) -> None:
    """Compare 7-day baseline and challenger with identical portfolio constraints."""
    global RUN_NAME, MODEL_FAMILY, STRATEGY, HORIZON_DAYS
    global INITIAL_CAPITAL, MAX_POSITIONS, MAX_POSITION_PCT, ALLOW_FRACTIONAL_SHARES

    original_config = (
        RUN_NAME,
        MODEL_FAMILY,
        STRATEGY,
        HORIZON_DAYS,
        INITIAL_CAPITAL,
        MAX_POSITIONS,
        MAX_POSITION_PCT,
        ALLOW_FRACTIONAL_SHARES,
    )
    try:
        STRATEGY = "forward_return_top10_buy"
        HORIZON_DAYS = 7
        INITIAL_CAPITAL = 5000.0
        MAX_POSITIONS = 10
        MAX_POSITION_PCT = 0.10
        ALLOW_FRACTIONAL_SHARES = False
        for variant in CHALLENGER_COMPARISON_VARIANTS:
            RUN_NAME = variant["run_name"]
            MODEL_FAMILY = variant["model_family"]
            logger.info(
                "Running matched portfolio comparison name=%s model=%s horizon=%d",
                RUN_NAME,
                MODEL_FAMILY,
                HORIZON_DAYS,
            )
            run_portfolio_backtest(**context)
    finally:
        (
            RUN_NAME,
            MODEL_FAMILY,
            STRATEGY,
            HORIZON_DAYS,
            INITIAL_CAPITAL,
            MAX_POSITIONS,
            MAX_POSITION_PCT,
            ALLOW_FRACTIONAL_SHARES,
        ) = original_config


def run_robustness_portfolio_comparison(**context) -> None:
    """Run matched whole-share portfolios from the longer robustness signals."""
    global RUN_NAME, MODEL_FAMILY, STRATEGY, HORIZON_DAYS
    global INITIAL_CAPITAL, MAX_POSITIONS, MAX_POSITION_PCT, ALLOW_FRACTIONAL_SHARES
    global DOWN_ENTRY_BLOCK_THRESHOLD, DOWN_EXIT_THRESHOLD

    original_config = (
        RUN_NAME,
        MODEL_FAMILY,
        STRATEGY,
        HORIZON_DAYS,
        INITIAL_CAPITAL,
        MAX_POSITIONS,
        MAX_POSITION_PCT,
        ALLOW_FRACTIONAL_SHARES,
        DOWN_ENTRY_BLOCK_THRESHOLD,
        DOWN_EXIT_THRESHOLD,
    )
    try:
        STRATEGY = "forward_return_top10_buy"
        HORIZON_DAYS = 7
        INITIAL_CAPITAL = 5000.0
        MAX_POSITIONS = 10
        MAX_POSITION_PCT = 0.10
        ALLOW_FRACTIONAL_SHARES = False
        DOWN_ENTRY_BLOCK_THRESHOLD = None
        DOWN_EXIT_THRESHOLD = None
        for variant in ROBUSTNESS_PORTFOLIO_VARIANTS:
            RUN_NAME = variant["run_name"]
            MODEL_FAMILY = variant["model_family"]
            logger.info(
                "Running robustness portfolio name=%s model=%s",
                RUN_NAME,
                MODEL_FAMILY,
            )
            run_portfolio_backtest(**context)
    finally:
        (
            RUN_NAME,
            MODEL_FAMILY,
            STRATEGY,
            HORIZON_DAYS,
            INITIAL_CAPITAL,
            MAX_POSITIONS,
            MAX_POSITION_PCT,
            ALLOW_FRACTIONAL_SHARES,
            DOWN_ENTRY_BLOCK_THRESHOLD,
            DOWN_EXIT_THRESHOLD,
        ) = original_config


DOWN_RISK_VARIANTS = (
    {
        "run_name": "portfolio_xgb_v3_7d_5k_down_control",
        "entry_block": None,
        "exit_threshold": None,
    },
    {
        "run_name": "portfolio_xgb_v3_7d_5k_down_entry_block_060",
        "entry_block": 0.60,
        "exit_threshold": None,
    },
    {
        "run_name": "portfolio_xgb_v3_7d_5k_down_entry_exit_060",
        "entry_block": 0.60,
        "exit_threshold": 0.60,
    },
)


def run_down_risk_filter_backtest(**context) -> None:
    """Test DOWN predictions as long-only risk controls; never opens shorts."""
    global RUN_NAME, MODEL_FAMILY, STRATEGY, HORIZON_DAYS
    global INITIAL_CAPITAL, MAX_POSITIONS, MAX_POSITION_PCT, ALLOW_FRACTIONAL_SHARES
    global DOWN_ENTRY_BLOCK_THRESHOLD, DOWN_EXIT_THRESHOLD

    original_config = (
        RUN_NAME,
        MODEL_FAMILY,
        STRATEGY,
        HORIZON_DAYS,
        INITIAL_CAPITAL,
        MAX_POSITIONS,
        MAX_POSITION_PCT,
        ALLOW_FRACTIONAL_SHARES,
        DOWN_ENTRY_BLOCK_THRESHOLD,
        DOWN_EXIT_THRESHOLD,
    )
    try:
        MODEL_FAMILY = "xgb_forward_v3_context_backtest"
        STRATEGY = "forward_return_top10_buy"
        HORIZON_DAYS = 7
        INITIAL_CAPITAL = 5000.0
        MAX_POSITIONS = 10
        MAX_POSITION_PCT = 0.10
        ALLOW_FRACTIONAL_SHARES = False
        for variant in DOWN_RISK_VARIANTS:
            RUN_NAME = variant["run_name"]
            DOWN_ENTRY_BLOCK_THRESHOLD = variant["entry_block"]
            DOWN_EXIT_THRESHOLD = variant["exit_threshold"]
            logger.info(
                "Running DOWN risk variant name=%s entry_block=%s exit=%s",
                RUN_NAME,
                DOWN_ENTRY_BLOCK_THRESHOLD,
                DOWN_EXIT_THRESHOLD,
            )
            run_portfolio_backtest(**context)
    finally:
        (
            RUN_NAME,
            MODEL_FAMILY,
            STRATEGY,
            HORIZON_DAYS,
            INITIAL_CAPITAL,
            MAX_POSITIONS,
            MAX_POSITION_PCT,
            ALLOW_FRACTIONAL_SHARES,
            DOWN_ENTRY_BLOCK_THRESHOLD,
            DOWN_EXIT_THRESHOLD,
        ) = original_config


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="eod_xgb_portfolio_backtest",
    default_args=default_args,
    start_date=pendulum.datetime(2026, 1, 1, tz=LOCAL_TZ),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["research", "portfolio", "backtest", "xgboost"],
) as dag:
    PythonOperator(
        task_id="run_portfolio_backtest",
        python_callable=run_portfolio_backtest,
    )


with DAG(
    dag_id="eod_xgb_portfolio_sizing_comparison",
    default_args=default_args,
    start_date=pendulum.datetime(2026, 1, 1, tz=LOCAL_TZ),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["research", "portfolio", "backtest", "xgboost", "sizing"],
) as sizing_comparison_dag:
    PythonOperator(
        task_id="run_sizing_comparison",
        python_callable=run_sizing_comparison,
    )


with DAG(
    dag_id="eod_xgb_challenger_portfolio_comparison",
    default_args=default_args,
    start_date=pendulum.datetime(2026, 1, 1, tz=LOCAL_TZ),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["research", "portfolio", "backtest", "xgboost", "challenger"],
) as challenger_portfolio_dag:
    PythonOperator(
        task_id="run_challenger_portfolio_comparison",
        python_callable=run_challenger_portfolio_comparison,
    )


with DAG(
    dag_id="eod_xgb_down_risk_filter_backtest",
    default_args=default_args,
    start_date=pendulum.datetime(2026, 1, 1, tz=LOCAL_TZ),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["research", "portfolio", "backtest", "xgboost", "down-risk"],
) as down_risk_filter_dag:
    PythonOperator(
        task_id="run_down_risk_filter_backtest",
        python_callable=run_down_risk_filter_backtest,
    )


with DAG(
    dag_id="eod_xgb_7d_robustness_portfolio_comparison",
    default_args=default_args,
    start_date=pendulum.datetime(2026, 1, 1, tz=LOCAL_TZ),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["research", "portfolio", "backtest", "xgboost", "robustness"],
) as robustness_portfolio_dag:
    PythonOperator(
        task_id="run_7d_robustness_portfolio_comparison",
        python_callable=run_robustness_portfolio_comparison,
    )
