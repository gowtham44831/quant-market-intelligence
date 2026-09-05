from __future__ import annotations

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta, timezone
import os
import logging

import psycopg2
from psycopg2.extras import execute_values

import pandas as pd


logger = logging.getLogger("airflow.task")
logger.setLevel(logging.INFO)

# -----------------------
# DB CONFIG (yours)
# -----------------------
DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "postgres"),
    "dbname": os.getenv("POSTGRES_DB", "stocks"),
    "user": os.getenv("POSTGRES_USER", "postgres"),
    "password": os.getenv("POSTGRES_PASSWORD", "postgres"),
    "port": int(os.getenv("POSTGRES_PORT", 5432)),
}

TICKERS_SQL = """
SELECT ticker
FROM (
    SELECT ticker FROM tickers_info WHERE sp500 = TRUE OR qqq = TRUE
    UNION SELECT unnest(ARRAY[
        'SPY','QQQ','XLC','XLY','XLP','XLE','XLF','XLV',
        'XLI','XLB','XLRE','XLK','XLU'
    ])
) t
ORDER BY ticker
"""

MARKET_ETFS = ("SPY", "QQQ")
SECTOR_ETFS = ("XLC", "XLY", "XLP", "XLE", "XLF", "XLV", "XLI", "XLB", "XLRE", "XLK", "XLU")

MARKET_CONTEXT_COLS = [
    "spy_return_1d", "spy_return_5d", "spy_return_20d", "spy_trend_50", "spy_volatility_20d",
    "qqq_return_1d", "qqq_return_5d", "qqq_return_20d", "qqq_trend_50", "qqq_volatility_20d",
    "sector_etf", "sector_return_1d", "sector_return_5d", "sector_return_20d",
    "sector_trend_50", "sector_volatility_20d",
    "stock_minus_spy_5d", "stock_minus_spy_20d",
    "stock_minus_qqq_5d", "stock_minus_qqq_20d",
    "stock_minus_sector_5d", "stock_minus_sector_20d",
]

# The 120d/240d names describe maximum indicator windows, not row retention.
# Retain enough trading observations for a two-year training window plus a
# 240-date walk-forward test and its 20-day realized-return tail.
FEATURE_RETENTION_TRADING_DAYS = int(
    os.getenv("FEATURE_RETENTION_TRADING_DAYS", "800")
)

# Calendar history fetched from source tables before indicators are calculated.
# Four years provides roughly 1,000 US trading sessions, leaving warm-up rows
# for 200-day indicators before the retained 800-row feature window.
FEATURE_SOURCE_CALENDAR_DAYS = int(
    os.getenv("FEATURE_SOURCE_CALENDAR_DAYS", "1460")
)

# -----------------------
# Table-specific schemas
# -----------------------
FEATURE_COLS_120 = [
    "trade_date", "ticker",
    "close_price", "vwap",
    "sma_50", "sma_100",
    "ema_50", "ema_100",
    "rsi_14", "rsi_30",
    "macd", "macd_signal", "macd_hist",
    "volatility_30d", "volatility_60d",
    "volume_zscore_30",
    "relative_volume_20", "atr_14",
    "support_20d", "support_60d", "support_120d",
    "resistance_20d", "resistance_60d", "resistance_120d",
    "distance_to_support_20d_atr", "distance_to_support_60d_atr", "distance_to_support_120d_atr",
    "distance_to_resistance_20d_atr", "distance_to_resistance_60d_atr", "distance_to_resistance_120d_atr",
    "breakdown_20d", "breakdown_60d", "breakdown_120d",
    "breakout_20d", "breakout_60d", "breakout_120d",
    *MARKET_CONTEXT_COLS,
    "forward_return_1d", "forward_return_5d", "forward_return_7d",
    "direction_1d", "direction_5d", "direction_7d",
]

FEATURE_COLS_240 = [
    "trade_date", "ticker",
    "close_price", "vwap",
    "sma_50", "sma_100", "sma_200",
    "ema_50", "ema_100", "ema_200",
    "rsi_14", "rsi_30", "rsi_60",
    "macd", "macd_signal", "macd_hist",
    "volatility_30d", "volatility_60d", "volatility_120d",
    "volume_zscore_30", "volume_zscore_60",
    "relative_volume_20", "atr_14",
    "support_20d", "support_60d", "support_120d",
    "resistance_20d", "resistance_60d", "resistance_120d",
    "distance_to_support_20d_atr", "distance_to_support_60d_atr", "distance_to_support_120d_atr",
    "distance_to_resistance_20d_atr", "distance_to_resistance_60d_atr", "distance_to_resistance_120d_atr",
    "breakdown_20d", "breakdown_60d", "breakdown_120d",
    "breakout_20d", "breakout_60d", "breakout_120d",
    *MARKET_CONTEXT_COLS,
    "forward_return_1d", "forward_return_5d", "forward_return_7d",
    "forward_return_10d", "forward_return_20d",
    "direction_1d", "direction_5d", "direction_7d",
    "direction_10d", "direction_20d",
]


# -----------------------
# Helpers
# -----------------------
def _utc_today_date() -> datetime.date:
    return datetime.now(timezone.utc).date()


def _nan_to_none(x):
    if x is None:
        return None
    if pd.isna(x):
        return None
    # psycopg2 does not reliably adapt NumPy scalar types.
    if hasattr(x, "item"):
        return x.item()
    return x


def get_cols_for_table(table_name: str) -> list[str]:
    if table_name == "daily_features_120d":
        return FEATURE_COLS_120
    if table_name == "daily_features_240d":
        return FEATURE_COLS_240
    raise ValueError(f"Unknown feature table: {table_name}")


def fetch_tickers(conn) -> list[str]:
    with conn.cursor() as cur:
        cur.execute(TICKERS_SQL)
        rows = cur.fetchall()
    return [r[0] for r in rows]


def fetch_sector_map(conn) -> dict[str, str]:
    with conn.cursor() as cur:
        cur.execute("SELECT ticker, sector_etf FROM ticker_sector_map")
        return dict(cur.fetchall())


def load_daily(conn, ticker: str, start_date) -> pd.DataFrame:
    sql = """
        SELECT d.trade_date, d.high, d.low, d."close" as close_price, d.vwap, d.volume
        FROM daily_market_summary d
        LEFT JOIN ticker_identity_boundaries b ON b.ticker = d.ticker
        WHERE d.ticker = %s
          AND d.trade_date >= %s
          AND d.trade_date >= COALESCE(b.valid_from, d.trade_date)
        ORDER BY trade_date ASC
    """
    return pd.read_sql(sql, conn, params=(ticker, start_date))


def load_intraday(conn, ticker: str, start_ts: datetime) -> pd.DataFrame:
    sql = """
        SELECT i.ts, i.high, i.low, i."close" as close_price, i.vwap, i.volume
        FROM intraday_data i
        LEFT JOIN ticker_identity_boundaries b ON b.ticker = i.ticker
        WHERE i.ticker = %s
          AND i.ts >= %s
          AND i.ts::date >= COALESCE(b.valid_from, i.ts::date)
        ORDER BY ts ASC
    """
    return pd.read_sql(sql, conn, params=(ticker, start_ts))


def intraday_to_daily(intra: pd.DataFrame) -> pd.DataFrame:
    """
    Convert intraday bars into OHLC-derived daily inputs.
    """
    if intra.empty:
        return intra

    intra = intra.copy()
    intra["ts"] = pd.to_datetime(intra["ts"])
    intra["trade_date"] = intra["ts"].dt.date

    intra = intra.sort_values("ts")
    grouped = intra.groupby("trade_date", sort=True)
    daily_last = grouped.agg(
        high=("high", "max"),
        low=("low", "min"),
        close_price=("close_price", "last"),
        vwap=("vwap", "last"),
        volume=("volume", "sum"),
    ).reset_index()
    daily_last = daily_last.sort_values("trade_date").reset_index(drop=True)
    return daily_last


def combine_daily_and_intraday(daily: pd.DataFrame, intra_daily: pd.DataFrame) -> pd.DataFrame:
    daily = daily.copy()
    if daily.empty and intra_daily.empty:
        return pd.DataFrame(columns=["trade_date", "high", "low", "close_price", "vwap", "volume"])

    if not daily.empty:
        daily["trade_date"] = pd.to_datetime(daily["trade_date"]).dt.date

    if intra_daily.empty:
        base = daily
    else:
        base = pd.merge(
            daily,
            intra_daily,
            on="trade_date",
            how="outer",
            suffixes=("_daily", "_intra")
        )

        def pick(col):
            # Adjusted EOD data is authoritative for completed sessions.
            # Intraday data only fills a date not yet present in the daily table.
            return base[f"{col}_daily"].combine_first(base[f"{col}_intra"])

        base = pd.DataFrame({
            "trade_date": base["trade_date"],
            "high": pick("high"),
            "low": pick("low"),
            "close_price": pick("close_price"),
            "vwap": pick("vwap"),
            "volume": pick("volume"),
        })

    base = base.dropna(subset=["trade_date"]).sort_values("trade_date").reset_index(drop=True)
    return base


def calc_sma(series: pd.Series, window: int) -> pd.Series:
    return series.rolling(window=window, min_periods=window).mean()


def calc_ema(series: pd.Series, span: int) -> pd.Series:
    return series.ewm(span=span, adjust=False, min_periods=span).mean()


def calc_rsi(close: pd.Series, period: int) -> pd.Series:
    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = (-delta).clip(lower=0)

    avg_gain = gain.ewm(alpha=1/period, adjust=False, min_periods=period).mean()
    avg_loss = loss.ewm(alpha=1/period, adjust=False, min_periods=period).mean()

    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return rsi


def calc_macd(close: pd.Series, fast=12, slow=26, signal=9):
    ema_fast = calc_ema(close, fast)
    ema_slow = calc_ema(close, slow)
    macd = ema_fast - ema_slow
    macd_signal = macd.ewm(span=signal, adjust=False, min_periods=signal).mean()
    macd_hist = macd - macd_signal
    return macd, macd_signal, macd_hist


def calc_volatility(close: pd.Series, window: int) -> pd.Series:
    ret = close.pct_change()
    return ret.rolling(window=window, min_periods=window).std()


def calc_volume_zscore(volume: pd.Series, window: int) -> pd.Series:
    mean = volume.rolling(window=window, min_periods=window).mean()
    std = volume.rolling(window=window, min_periods=window).std()
    return (volume - mean) / std


def calc_atr(high: pd.Series, low: pd.Series, close: pd.Series, window: int = 14) -> pd.Series:
    """Wilder-style average true range using information available through each close."""
    previous_close = close.shift(1)
    true_range = pd.concat(
        [high - low, (high - previous_close).abs(), (low - previous_close).abs()],
        axis=1,
    ).max(axis=1)
    return true_range.ewm(alpha=1 / window, adjust=False, min_periods=window).mean()


def build_context_frame(daily: pd.DataFrame, prefix: str) -> pd.DataFrame:
    """Build backward-looking context from finalized daily ETF bars."""
    columns = [
        "trade_date", f"{prefix}_return_1d", f"{prefix}_return_5d",
        f"{prefix}_return_20d", f"{prefix}_trend_50", f"{prefix}_volatility_20d",
    ]
    if daily.empty:
        return pd.DataFrame(columns=columns)

    frame = daily[["trade_date", "close_price"]].copy().sort_values("trade_date")
    frame["trade_date"] = pd.to_datetime(frame["trade_date"]).dt.date
    close = pd.to_numeric(frame["close_price"], errors="coerce")
    returns_1d = close.pct_change()
    sma_50 = close.rolling(50, min_periods=50).mean()
    frame[f"{prefix}_return_1d"] = returns_1d
    frame[f"{prefix}_return_5d"] = close.pct_change(5)
    frame[f"{prefix}_return_20d"] = close.pct_change(20)
    frame[f"{prefix}_trend_50"] = close / sma_50 - 1
    frame[f"{prefix}_volatility_20d"] = returns_1d.rolling(20, min_periods=20).std()
    return frame[columns]


def attach_market_context(
    base: pd.DataFrame,
    ticker: str,
    context_frames: dict[str, pd.DataFrame],
    sector_map: dict[str, str],
) -> pd.DataFrame:
    result = base.copy()
    result["stock_return_5d_tmp"] = result["close_price"].pct_change(5)
    result["stock_return_20d_tmp"] = result["close_price"].pct_change(20)

    for market_ticker in MARKET_ETFS:
        prefix = market_ticker.lower()
        result = result.merge(context_frames[market_ticker], on="trade_date", how="left")
        result[f"stock_minus_{prefix}_5d"] = (
            result["stock_return_5d_tmp"] - result[f"{prefix}_return_5d"]
        )
        result[f"stock_minus_{prefix}_20d"] = (
            result["stock_return_20d_tmp"] - result[f"{prefix}_return_20d"]
        )

    sector_etf = sector_map.get(ticker)
    result["sector_etf"] = sector_etf
    if sector_etf and sector_etf in context_frames:
        sector = context_frames[sector_etf].rename(columns={
            f"{sector_etf.lower()}_return_1d": "sector_return_1d",
            f"{sector_etf.lower()}_return_5d": "sector_return_5d",
            f"{sector_etf.lower()}_return_20d": "sector_return_20d",
            f"{sector_etf.lower()}_trend_50": "sector_trend_50",
            f"{sector_etf.lower()}_volatility_20d": "sector_volatility_20d",
        })
        result = result.merge(sector, on="trade_date", how="left")
        result["stock_minus_sector_5d"] = result["stock_return_5d_tmp"] - result["sector_return_5d"]
        result["stock_minus_sector_20d"] = result["stock_return_20d_tmp"] - result["sector_return_20d"]

    return result.drop(columns=["stock_return_5d_tmp", "stock_return_20d_tmp"])


def compute_features_frame(base: pd.DataFrame, table_name: str) -> pd.DataFrame:
    """
    base columns: trade_date, high, low, close_price, vwap, volume (sorted)
    Trading-day forward returns are computed via shift(-N) since the index is trading days.
    """
    cols = get_cols_for_table(table_name)
    if base.empty:
        return pd.DataFrame(columns=cols)

    df = base.copy()
    df["close_price"] = pd.to_numeric(df["close_price"], errors="coerce")
    df["high"] = pd.to_numeric(df["high"], errors="coerce")
    df["low"] = pd.to_numeric(df["low"], errors="coerce")
    df["vwap"] = pd.to_numeric(df["vwap"], errors="coerce")
    df["volume"] = pd.to_numeric(df["volume"], errors="coerce")

    close = df["close_price"]
    vol = df["volume"]
    high = df["high"]
    low = df["low"]

    # --- shared indicators ---
    df["sma_50"] = calc_sma(close, 50)
    df["sma_100"] = calc_sma(close, 100)
    df["ema_50"] = calc_ema(close, 50)
    df["ema_100"] = calc_ema(close, 100)

    df["rsi_14"] = calc_rsi(close, 14)
    df["rsi_30"] = calc_rsi(close, 30)

    macd, macd_sig, macd_hist = calc_macd(close, 12, 26, 9)
    df["macd"] = macd
    df["macd_signal"] = macd_sig
    df["macd_hist"] = macd_hist

    df["volatility_30d"] = calc_volatility(close, 30)
    df["volatility_60d"] = calc_volatility(close, 60)

    df["volume_zscore_30"] = calc_volume_zscore(vol, 30)

    # Support/resistance levels exclude the current session to avoid defining a
    # level with the same close that is being tested against it.
    df["relative_volume_20"] = vol / vol.shift(1).rolling(20, min_periods=20).mean()
    df["atr_14"] = calc_atr(high, low, close, 14)
    safe_atr = df["atr_14"].where(df["atr_14"] > 0)
    for window in (20, 60, 120):
        support_col = f"support_{window}d"
        resistance_col = f"resistance_{window}d"
        df[support_col] = low.shift(1).rolling(window, min_periods=window).min()
        df[resistance_col] = high.shift(1).rolling(window, min_periods=window).max()
        df[f"distance_to_support_{window}d_atr"] = (close - df[support_col]) / safe_atr
        df[f"distance_to_resistance_{window}d_atr"] = (df[resistance_col] - close) / safe_atr
        df[f"breakdown_{window}d"] = close.lt(df[support_col]).where(df[support_col].notna()).astype("boolean")
        df[f"breakout_{window}d"] = close.gt(df[resistance_col]).where(df[resistance_col].notna()).astype("boolean")

    df.replace([float("inf"), float("-inf")], float("nan"), inplace=True)

    # --- extras for 240d ---
    if table_name == "daily_features_240d":
        df["sma_200"] = calc_sma(close, 200)
        df["ema_200"] = calc_ema(close, 200)
        df["rsi_60"] = calc_rsi(close, 60)
        df["volatility_120d"] = calc_volatility(close, 120)
        df["volume_zscore_60"] = calc_volume_zscore(vol, 60)

    # --- forward returns (TRADING DAYS) ---
    df["forward_return_1d"] = close.shift(-1) / close - 1
    df["forward_return_5d"] = close.shift(-5) / close - 1
    df["forward_return_7d"] = close.shift(-7) / close - 1

    if table_name == "daily_features_240d":
        df["forward_return_10d"] = close.shift(-10) / close - 1
        df["forward_return_20d"] = close.shift(-20) / close - 1

    # --- directions ---
    df["direction_1d"] = (df["forward_return_1d"] > 0).astype("int16")
    df["direction_5d"] = (df["forward_return_5d"] > 0).astype("int16")
    df["direction_7d"] = (df["forward_return_7d"] > 0).astype("int16")

    if table_name == "daily_features_240d":
        df["direction_10d"] = (df["forward_return_10d"] > 0).astype("int16")
        df["direction_20d"] = (df["forward_return_20d"] > 0).astype("int16")

    # Ensure we return only the expected columns (some may not exist in df for 120d)
    for c in cols:
        if c not in df.columns:
            df[c] = None

    return df[cols]


def filter_label_ready_rows(features: pd.DataFrame, table_name: str) -> pd.DataFrame:
    """
    Keep rows where the max-horizon label exists (training-ready rows).
    This prevents overwriting labels with NULLs near the end of the series.
    """
    if features.empty:
        return features
    if table_name == "daily_features_240d":
        # for 240d, we want 20d label to exist for training
        return features[features["forward_return_20d"].notna()].copy()
    # for 120d, we want 7d label to exist
    return features[features["forward_return_7d"].notna()].copy()


def generate_features_for_lookback(indicator_window_days: int, table_name: str):
    if FEATURE_RETENTION_TRADING_DAYS <= 0 or FEATURE_SOURCE_CALENDAR_DAYS <= 0:
        raise ValueError("Feature retention and source lookback must be positive.")

    today = _utc_today_date()
    start_date = today - timedelta(days=FEATURE_SOURCE_CALENDAR_DAYS)
    start_ts = datetime.combine(start_date, datetime.min.time())

    conn = psycopg2.connect(**DB_CONFIG)
    try:
        tickers = fetch_tickers(conn)
        sector_map = fetch_sector_map(conn)
        context_frames = {}
        for context_ticker in (*MARKET_ETFS, *SECTOR_ETFS):
            context_daily = load_daily(conn, context_ticker, start_date)
            context_frames[context_ticker] = build_context_frame(
                context_daily, context_ticker.lower()
            )
        logger.info("Total tickers selected = %d", len(tickers))

        for i, ticker in enumerate(tickers, start=1):
            try:
                logger.info(
                    "[%d/%d] Processing %s for %s (indicator_window=%dd retention=%d)",
                    i, len(tickers), ticker, table_name, indicator_window_days,
                    FEATURE_RETENTION_TRADING_DAYS,
                )

                daily_df = load_daily(conn, ticker, start_date)
                intra_df = load_intraday(conn, ticker, start_ts)

                intra_daily = intraday_to_daily(intra_df)
                base = combine_daily_and_intraday(daily_df, intra_daily)
                base = attach_market_context(base, ticker, context_frames, sector_map)

                if base.empty or base["close_price"].dropna().empty:
                    logger.warning("No price data for %s, skipping", ticker)
                    continue

                feats = compute_features_frame(base, table_name)

                # Keep only valid rows (trade_date + close required)
                feats = feats.dropna(subset=["trade_date", "close_price"])

                upsert_features(
                    conn,
                    table_name,
                    ticker,
                    feats,
                    keep_last_n_days=FEATURE_RETENTION_TRADING_DAYS,
                )

            except Exception as e:
                conn.rollback()
                logger.exception("Ticker %s failed: %s", ticker, e)

    finally:
        conn.close()


def upsert_features(conn, table: str, ticker: str, features: pd.DataFrame, keep_last_n_days: int):
    """Atomically replace one ticker with its exact retained feature window."""
    if features.empty:
        return

    cols = get_cols_for_table(table)

    features = features.sort_values("trade_date").tail(keep_last_n_days).copy()
    features["ticker"] = ticker

    records = []
    for _, r in features.iterrows():
        rec = {c: _nan_to_none(r.get(c)) for c in cols}
        records.append(rec)

    if not records:
        return

    col_list = ", ".join(cols)

    insert_sql = f"""
        INSERT INTO {table} ({col_list})
        VALUES %s
    """

    values = [tuple(rec[c] for c in cols) for rec in records]

    with conn.cursor() as cur:
        # Deleting and inserting in one transaction removes obsolete isolated
        # dates while preserving the previous data if the insert fails.
        cur.execute(f"DELETE FROM {table} WHERE ticker = %s", (ticker,))
        execute_values(cur, insert_sql, values, page_size=500)
    conn.commit()



# -----------------------
# Airflow DAG
# -----------------------
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="stock_indicators_dag",
    default_args=default_args,
    start_date=datetime(2025, 12, 19),
    schedule="0 22 * * 1-5",  # 22:00 UTC Mon-Fri
    catchup=False,
    max_active_runs=1,
    tags=["production", "features", "stocks", "sequential"],
) as dag:

    generate_120d_features = PythonOperator(
        task_id="generate_120d_features",
        python_callable=lambda: generate_features_for_lookback(120, "daily_features_120d"),
    )

    generate_240d_features = PythonOperator(
        task_id="generate_240d_features",
        python_callable=lambda: generate_features_for_lookback(240, "daily_features_240d"),
    )

    generate_120d_features >> generate_240d_features
