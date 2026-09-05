from __future__ import annotations

import os
import logging
from datetime import timedelta

import pendulum
import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator

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

DDL_STATEMENTS: list[str] = [
    # ---------------- tickers_info ----------------
    """
    CREATE TABLE IF NOT EXISTS public.tickers_info (
        ticker text NOT NULL,
        "name" text NULL,
        market text NULL,
        locale text NULL,
        primary_exchange text NULL,
        "type" text NULL,
        active bool NULL,
        currency_name text NULL,
        cik text NULL,
        composite_figi text NULL,
        share_class_figi text NULL,
        sp500 bool DEFAULT false NULL,
        sp500_weight numeric(8, 4) NULL,
        qqq bool DEFAULT false NULL,
        qqq_weight numeric(8, 4) NULL,
        last_updated_utc timestamptz NULL,
        CONSTRAINT tickers_info_pkey PRIMARY KEY (ticker)
    );
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_tickers_sp500_active
    ON public.tickers_info USING btree (sp500, active);
    """,

    # Point-in-time sector classification can later be versioned here without
    # coupling it to the vendor's ticker reference payload.
    """
    CREATE TABLE IF NOT EXISTS public.ticker_sector_map (
        ticker varchar(20) NOT NULL,
        sector text NOT NULL,
        sector_etf varchar(10) NOT NULL,
        source text NULL,
        updated_at timestamptz NOT NULL DEFAULT now(),
        CONSTRAINT ticker_sector_map_pkey PRIMARY KEY (ticker)
    );
    """,

    # Current-symbol history must not include an older security that reused
    # the same ticker. valid_from is the first valid date for today's identity.
    """
    CREATE TABLE IF NOT EXISTS public.ticker_identity_boundaries (
        ticker varchar(20) NOT NULL,
        valid_from date NOT NULL,
        reason text NULL,
        updated_at timestamptz NOT NULL DEFAULT now(),
        CONSTRAINT ticker_identity_boundaries_pkey PRIMARY KEY (ticker)
    );
    """,
    """
    INSERT INTO public.ticker_identity_boundaries (ticker, valid_from, reason)
    VALUES
        ('BNY', DATE '2026-05-21', 'Ticker reused; previous security ended 2026-02-06'),
        ('SPCX', DATE '2026-06-12', 'Ticker reused; previous security ended 2026-04-06')
    ON CONFLICT (ticker) DO UPDATE SET
        valid_from = EXCLUDED.valid_from,
        reason = EXCLUDED.reason,
        updated_at = now();
    """,

    # ---------------- daily_market_summary ----------------
    """
    CREATE TABLE IF NOT EXISTS public.daily_market_summary (
        trade_date date NOT NULL,
        ticker text NOT NULL,
        "open" numeric NULL,
        high numeric NULL,
        low numeric NULL,
        "close" numeric NULL,
        volume numeric NULL,
        vwap numeric NULL,
        trade_count int4 NULL,
        "timestamp" int8 NULL,
        CONSTRAINT daily_market_summary_pkey PRIMARY KEY (trade_date, ticker)
    );
    """,

    # ---------------- daily_features_120d ----------------
    """
    CREATE TABLE IF NOT EXISTS public.daily_features_120d (
        trade_date date NOT NULL,
        ticker varchar(20) NOT NULL,
        close_price numeric(18, 6) NULL,
        vwap numeric(18, 6) NULL,
        sma_50 numeric(18, 6) NULL,
        sma_100 numeric(18, 6) NULL,
        ema_50 numeric(18, 6) NULL,
        ema_100 numeric(18, 6) NULL,
        rsi_14 numeric(18, 6) NULL,
        rsi_30 numeric(18, 6) NULL,
        macd numeric(18, 6) NULL,
        macd_signal numeric(18, 6) NULL,
        macd_hist numeric(18, 6) NULL,
        volatility_30d numeric(18, 6) NULL,
        volatility_60d numeric(18, 6) NULL,
        volume_zscore_30 numeric(18, 6) NULL,
        relative_volume_20 numeric(18, 6) NULL,
        atr_14 numeric(18, 6) NULL,
        support_20d numeric(18, 6) NULL,
        support_60d numeric(18, 6) NULL,
        support_120d numeric(18, 6) NULL,
        resistance_20d numeric(18, 6) NULL,
        resistance_60d numeric(18, 6) NULL,
        resistance_120d numeric(18, 6) NULL,
        distance_to_support_20d_atr numeric(18, 6) NULL,
        distance_to_support_60d_atr numeric(18, 6) NULL,
        distance_to_support_120d_atr numeric(18, 6) NULL,
        distance_to_resistance_20d_atr numeric(18, 6) NULL,
        distance_to_resistance_60d_atr numeric(18, 6) NULL,
        distance_to_resistance_120d_atr numeric(18, 6) NULL,
        breakdown_20d bool NULL,
        breakdown_60d bool NULL,
        breakdown_120d bool NULL,
        breakout_20d bool NULL,
        breakout_60d bool NULL,
        breakout_120d bool NULL,
        spy_return_1d numeric(18, 6) NULL,
        spy_return_5d numeric(18, 6) NULL,
        spy_return_20d numeric(18, 6) NULL,
        spy_trend_50 numeric(18, 6) NULL,
        spy_volatility_20d numeric(18, 6) NULL,
        qqq_return_1d numeric(18, 6) NULL,
        qqq_return_5d numeric(18, 6) NULL,
        qqq_return_20d numeric(18, 6) NULL,
        qqq_trend_50 numeric(18, 6) NULL,
        qqq_volatility_20d numeric(18, 6) NULL,
        sector_etf varchar(10) NULL,
        sector_return_1d numeric(18, 6) NULL,
        sector_return_5d numeric(18, 6) NULL,
        sector_return_20d numeric(18, 6) NULL,
        sector_trend_50 numeric(18, 6) NULL,
        sector_volatility_20d numeric(18, 6) NULL,
        stock_minus_spy_5d numeric(18, 6) NULL,
        stock_minus_spy_20d numeric(18, 6) NULL,
        stock_minus_qqq_5d numeric(18, 6) NULL,
        stock_minus_qqq_20d numeric(18, 6) NULL,
        stock_minus_sector_5d numeric(18, 6) NULL,
        stock_minus_sector_20d numeric(18, 6) NULL,
        forward_return_1d numeric(18, 6) NULL,
        forward_return_5d numeric(18, 6) NULL,
        forward_return_7d numeric(18, 6) NULL,
        direction_1d int2 NULL,
        direction_5d int2 NULL,
        direction_7d int2 NULL,
        CONSTRAINT daily_features_120d_pk PRIMARY KEY (trade_date, ticker)
    );
    """,
    """
    ALTER TABLE public.daily_features_120d
        ADD COLUMN IF NOT EXISTS relative_volume_20 numeric(18, 6),
        ADD COLUMN IF NOT EXISTS atr_14 numeric(18, 6),
        ADD COLUMN IF NOT EXISTS support_20d numeric(18, 6),
        ADD COLUMN IF NOT EXISTS support_60d numeric(18, 6),
        ADD COLUMN IF NOT EXISTS support_120d numeric(18, 6),
        ADD COLUMN IF NOT EXISTS resistance_20d numeric(18, 6),
        ADD COLUMN IF NOT EXISTS resistance_60d numeric(18, 6),
        ADD COLUMN IF NOT EXISTS resistance_120d numeric(18, 6),
        ADD COLUMN IF NOT EXISTS distance_to_support_20d_atr numeric(18, 6),
        ADD COLUMN IF NOT EXISTS distance_to_support_60d_atr numeric(18, 6),
        ADD COLUMN IF NOT EXISTS distance_to_support_120d_atr numeric(18, 6),
        ADD COLUMN IF NOT EXISTS distance_to_resistance_20d_atr numeric(18, 6),
        ADD COLUMN IF NOT EXISTS distance_to_resistance_60d_atr numeric(18, 6),
        ADD COLUMN IF NOT EXISTS distance_to_resistance_120d_atr numeric(18, 6),
        ADD COLUMN IF NOT EXISTS breakdown_20d bool,
        ADD COLUMN IF NOT EXISTS breakdown_60d bool,
        ADD COLUMN IF NOT EXISTS breakdown_120d bool,
        ADD COLUMN IF NOT EXISTS breakout_20d bool,
        ADD COLUMN IF NOT EXISTS breakout_60d bool,
        ADD COLUMN IF NOT EXISTS breakout_120d bool,
        ADD COLUMN IF NOT EXISTS spy_return_1d numeric(18, 6),
        ADD COLUMN IF NOT EXISTS spy_return_5d numeric(18, 6),
        ADD COLUMN IF NOT EXISTS spy_return_20d numeric(18, 6),
        ADD COLUMN IF NOT EXISTS spy_trend_50 numeric(18, 6),
        ADD COLUMN IF NOT EXISTS spy_volatility_20d numeric(18, 6),
        ADD COLUMN IF NOT EXISTS qqq_return_1d numeric(18, 6),
        ADD COLUMN IF NOT EXISTS qqq_return_5d numeric(18, 6),
        ADD COLUMN IF NOT EXISTS qqq_return_20d numeric(18, 6),
        ADD COLUMN IF NOT EXISTS qqq_trend_50 numeric(18, 6),
        ADD COLUMN IF NOT EXISTS qqq_volatility_20d numeric(18, 6),
        ADD COLUMN IF NOT EXISTS sector_etf varchar(10),
        ADD COLUMN IF NOT EXISTS sector_return_1d numeric(18, 6),
        ADD COLUMN IF NOT EXISTS sector_return_5d numeric(18, 6),
        ADD COLUMN IF NOT EXISTS sector_return_20d numeric(18, 6),
        ADD COLUMN IF NOT EXISTS sector_trend_50 numeric(18, 6),
        ADD COLUMN IF NOT EXISTS sector_volatility_20d numeric(18, 6),
        ADD COLUMN IF NOT EXISTS stock_minus_spy_5d numeric(18, 6),
        ADD COLUMN IF NOT EXISTS stock_minus_spy_20d numeric(18, 6),
        ADD COLUMN IF NOT EXISTS stock_minus_qqq_5d numeric(18, 6),
        ADD COLUMN IF NOT EXISTS stock_minus_qqq_20d numeric(18, 6),
        ADD COLUMN IF NOT EXISTS stock_minus_sector_5d numeric(18, 6),
        ADD COLUMN IF NOT EXISTS stock_minus_sector_20d numeric(18, 6);
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_daily_features_120d_ticker_date
    ON public.daily_features_120d USING btree (ticker, trade_date DESC);
    """,

    # ---------------- daily_features_240d ----------------
    """
    CREATE TABLE IF NOT EXISTS public.daily_features_240d (
        trade_date date NOT NULL,
        ticker varchar(20) NOT NULL,
        close_price numeric(18, 6) NULL,
        vwap numeric(18, 6) NULL,
        sma_50 numeric(18, 6) NULL,
        sma_100 numeric(18, 6) NULL,
        sma_200 numeric(18, 6) NULL,
        ema_50 numeric(18, 6) NULL,
        ema_100 numeric(18, 6) NULL,
        ema_200 numeric(18, 6) NULL,
        rsi_14 numeric(18, 6) NULL,
        rsi_30 numeric(18, 6) NULL,
        rsi_60 numeric(18, 6) NULL,
        macd numeric(18, 6) NULL,
        macd_signal numeric(18, 6) NULL,
        macd_hist numeric(18, 6) NULL,
        volatility_30d numeric(18, 6) NULL,
        volatility_60d numeric(18, 6) NULL,
        volatility_120d numeric(18, 6) NULL,
        volume_zscore_30 numeric(18, 6) NULL,
        volume_zscore_60 numeric(18, 6) NULL,
        relative_volume_20 numeric(18, 6) NULL,
        atr_14 numeric(18, 6) NULL,
        support_20d numeric(18, 6) NULL,
        support_60d numeric(18, 6) NULL,
        support_120d numeric(18, 6) NULL,
        resistance_20d numeric(18, 6) NULL,
        resistance_60d numeric(18, 6) NULL,
        resistance_120d numeric(18, 6) NULL,
        distance_to_support_20d_atr numeric(18, 6) NULL,
        distance_to_support_60d_atr numeric(18, 6) NULL,
        distance_to_support_120d_atr numeric(18, 6) NULL,
        distance_to_resistance_20d_atr numeric(18, 6) NULL,
        distance_to_resistance_60d_atr numeric(18, 6) NULL,
        distance_to_resistance_120d_atr numeric(18, 6) NULL,
        breakdown_20d bool NULL,
        breakdown_60d bool NULL,
        breakdown_120d bool NULL,
        breakout_20d bool NULL,
        breakout_60d bool NULL,
        breakout_120d bool NULL,
        spy_return_1d numeric(18, 6) NULL,
        spy_return_5d numeric(18, 6) NULL,
        spy_return_20d numeric(18, 6) NULL,
        spy_trend_50 numeric(18, 6) NULL,
        spy_volatility_20d numeric(18, 6) NULL,
        qqq_return_1d numeric(18, 6) NULL,
        qqq_return_5d numeric(18, 6) NULL,
        qqq_return_20d numeric(18, 6) NULL,
        qqq_trend_50 numeric(18, 6) NULL,
        qqq_volatility_20d numeric(18, 6) NULL,
        sector_etf varchar(10) NULL,
        sector_return_1d numeric(18, 6) NULL,
        sector_return_5d numeric(18, 6) NULL,
        sector_return_20d numeric(18, 6) NULL,
        sector_trend_50 numeric(18, 6) NULL,
        sector_volatility_20d numeric(18, 6) NULL,
        stock_minus_spy_5d numeric(18, 6) NULL,
        stock_minus_spy_20d numeric(18, 6) NULL,
        stock_minus_qqq_5d numeric(18, 6) NULL,
        stock_minus_qqq_20d numeric(18, 6) NULL,
        stock_minus_sector_5d numeric(18, 6) NULL,
        stock_minus_sector_20d numeric(18, 6) NULL,
        forward_return_1d numeric(18, 6) NULL,
        forward_return_5d numeric(18, 6) NULL,
        forward_return_7d numeric(18, 6) NULL,
        forward_return_10d numeric(18, 6) NULL,
        forward_return_20d numeric(18, 6) NULL,
        direction_1d int2 NULL,
        direction_5d int2 NULL,
        direction_7d int2 NULL,
        direction_10d int2 NULL,
        direction_20d int2 NULL,
        CONSTRAINT daily_features_240d_pk PRIMARY KEY (trade_date, ticker)
    );
    """,
    """
    ALTER TABLE public.daily_features_240d
        ADD COLUMN IF NOT EXISTS relative_volume_20 numeric(18, 6),
        ADD COLUMN IF NOT EXISTS atr_14 numeric(18, 6),
        ADD COLUMN IF NOT EXISTS support_20d numeric(18, 6),
        ADD COLUMN IF NOT EXISTS support_60d numeric(18, 6),
        ADD COLUMN IF NOT EXISTS support_120d numeric(18, 6),
        ADD COLUMN IF NOT EXISTS resistance_20d numeric(18, 6),
        ADD COLUMN IF NOT EXISTS resistance_60d numeric(18, 6),
        ADD COLUMN IF NOT EXISTS resistance_120d numeric(18, 6),
        ADD COLUMN IF NOT EXISTS distance_to_support_20d_atr numeric(18, 6),
        ADD COLUMN IF NOT EXISTS distance_to_support_60d_atr numeric(18, 6),
        ADD COLUMN IF NOT EXISTS distance_to_support_120d_atr numeric(18, 6),
        ADD COLUMN IF NOT EXISTS distance_to_resistance_20d_atr numeric(18, 6),
        ADD COLUMN IF NOT EXISTS distance_to_resistance_60d_atr numeric(18, 6),
        ADD COLUMN IF NOT EXISTS distance_to_resistance_120d_atr numeric(18, 6),
        ADD COLUMN IF NOT EXISTS breakdown_20d bool,
        ADD COLUMN IF NOT EXISTS breakdown_60d bool,
        ADD COLUMN IF NOT EXISTS breakdown_120d bool,
        ADD COLUMN IF NOT EXISTS breakout_20d bool,
        ADD COLUMN IF NOT EXISTS breakout_60d bool,
        ADD COLUMN IF NOT EXISTS breakout_120d bool,
        ADD COLUMN IF NOT EXISTS spy_return_1d numeric(18, 6),
        ADD COLUMN IF NOT EXISTS spy_return_5d numeric(18, 6),
        ADD COLUMN IF NOT EXISTS spy_return_20d numeric(18, 6),
        ADD COLUMN IF NOT EXISTS spy_trend_50 numeric(18, 6),
        ADD COLUMN IF NOT EXISTS spy_volatility_20d numeric(18, 6),
        ADD COLUMN IF NOT EXISTS qqq_return_1d numeric(18, 6),
        ADD COLUMN IF NOT EXISTS qqq_return_5d numeric(18, 6),
        ADD COLUMN IF NOT EXISTS qqq_return_20d numeric(18, 6),
        ADD COLUMN IF NOT EXISTS qqq_trend_50 numeric(18, 6),
        ADD COLUMN IF NOT EXISTS qqq_volatility_20d numeric(18, 6),
        ADD COLUMN IF NOT EXISTS sector_etf varchar(10),
        ADD COLUMN IF NOT EXISTS sector_return_1d numeric(18, 6),
        ADD COLUMN IF NOT EXISTS sector_return_5d numeric(18, 6),
        ADD COLUMN IF NOT EXISTS sector_return_20d numeric(18, 6),
        ADD COLUMN IF NOT EXISTS sector_trend_50 numeric(18, 6),
        ADD COLUMN IF NOT EXISTS sector_volatility_20d numeric(18, 6),
        ADD COLUMN IF NOT EXISTS stock_minus_spy_5d numeric(18, 6),
        ADD COLUMN IF NOT EXISTS stock_minus_spy_20d numeric(18, 6),
        ADD COLUMN IF NOT EXISTS stock_minus_qqq_5d numeric(18, 6),
        ADD COLUMN IF NOT EXISTS stock_minus_qqq_20d numeric(18, 6),
        ADD COLUMN IF NOT EXISTS stock_minus_sector_5d numeric(18, 6),
        ADD COLUMN IF NOT EXISTS stock_minus_sector_20d numeric(18, 6);
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_daily_features_240d_ticker_date
    ON public.daily_features_240d USING btree (ticker, trade_date DESC);
    """,

    # ---------------- intraday_data (partitioned) ----------------
    """
    CREATE TABLE IF NOT EXISTS public.intraday_data (
        ticker varchar(20) NOT NULL,
        ts timestamptz NOT NULL,
        "open" numeric(18, 6) NULL,
        high numeric(18, 6) NULL,
        low numeric(18, 6) NULL,
        "close" numeric(18, 6) NULL,
        volume int8 NULL,
        vwap numeric NULL,
        trades int4 NULL,
        CONSTRAINT intraday_data_pkey PRIMARY KEY (ticker, ts)
    )
    PARTITION BY RANGE (ts);
    """,
    """
    CREATE TABLE IF NOT EXISTS public.intraday_data_default
    PARTITION OF public.intraday_data DEFAULT;
    """,

    # ---------------- model_predictions ----------------
    """
    CREATE TABLE IF NOT EXISTS public.model_predictions (
        as_of_date date NOT NULL,
        horizon_days int4 NOT NULL,
        ticker varchar(20) NOT NULL,
        prob_up numeric(10, 6) NOT NULL,
        prob_down numeric(10, 6) NULL,
        score numeric(12, 6) NOT NULL,
        model_name text NOT NULL,
        created_at timestamptz DEFAULT now() NOT NULL,
        CONSTRAINT model_predictions_pk PRIMARY KEY (as_of_date, horizon_days, ticker, model_name)
    );
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_model_predictions_date_horizon
    ON public.model_predictions USING btree (as_of_date DESC, horizon_days);
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_model_predictions_ticker_date
    ON public.model_predictions USING btree (ticker, as_of_date DESC);
    """,

    # ---------------- daily_trade_signals ----------------
    """
    CREATE TABLE IF NOT EXISTS public.daily_trade_signals (
        as_of_date date NOT NULL,
        ticker varchar(20) NOT NULL,
        horizon_days int4 NOT NULL,
        strategy text NOT NULL,
        signal varchar(10) NOT NULL,
        prob_up numeric(10, 6) NOT NULL,
        score numeric(12, 6) NOT NULL,
        volume_zscore_30 numeric(12, 6) NULL,
        reason text NULL,
        created_at timestamptz DEFAULT now() NOT NULL,
        CONSTRAINT daily_trade_signals_pk PRIMARY KEY (as_of_date, ticker, horizon_days, strategy)
    );
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_trade_signals_date_strategy
    ON public.daily_trade_signals USING btree (as_of_date DESC, strategy, horizon_days);
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_trade_signals_signal_date
    ON public.daily_trade_signals USING btree (signal, as_of_date DESC);
    """,
    # model_name records WHICH model version produced the official signal.
    # It is deliberately NOT part of the primary key: there must be exactly one
    # official production signal per (as_of_date, ticker, horizon_days, strategy).
    """
    ALTER TABLE public.daily_trade_signals
    ADD COLUMN IF NOT EXISTS model_name varchar(100) NOT NULL DEFAULT 'unknown';
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_trade_signals_model_name
    ON public.daily_trade_signals USING btree (model_name, as_of_date DESC, horizon_days);
    """,

    # ---------------- backtest_trade_signals ----------------
    # Research / walk-forward output ONLY. Never written by production daily DAGs.
    """
    CREATE TABLE IF NOT EXISTS public.backtest_trade_signals (
        as_of_date date NOT NULL,
        ticker varchar(20) NOT NULL,
        horizon_days int4 NOT NULL,
        strategy text NOT NULL,
        model_name varchar(100) NOT NULL,
        signal varchar(10) NOT NULL,
        prob_up numeric(10, 6) NOT NULL,
        score numeric(12, 6) NOT NULL,
        volume_zscore_30 numeric(12, 6) NULL,
        reason text NULL,
        created_at timestamptz DEFAULT now() NOT NULL,
        CONSTRAINT backtest_trade_signals_pk
            PRIMARY KEY (as_of_date, ticker, horizon_days, strategy, model_name)
    );
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_backtest_signals_date_strategy
    ON public.backtest_trade_signals USING btree (as_of_date DESC, strategy, horizon_days);
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_backtest_signals_model
    ON public.backtest_trade_signals USING btree (model_name, horizon_days, as_of_date DESC);
    """,
    """
    DO $$
    DECLARE
      current_pk text;
      current_pk_def text;
    BEGIN
      SELECT c.conname, pg_get_constraintdef(c.oid)
        INTO current_pk, current_pk_def
      FROM pg_constraint c
      WHERE c.conrelid = 'public.backtest_trade_signals'::regclass
        AND c.contype = 'p';
      IF current_pk_def IS NULL OR current_pk_def NOT LIKE '%model_name%' THEN
        IF current_pk IS NOT NULL THEN
          EXECUTE format('ALTER TABLE public.backtest_trade_signals DROP CONSTRAINT %I', current_pk);
        END IF;
        ALTER TABLE public.backtest_trade_signals
          ADD CONSTRAINT backtest_trade_signals_pk PRIMARY KEY
          (as_of_date, ticker, horizon_days, strategy, model_name);
      END IF;
    END $$;
    """,

    """
    CREATE TABLE IF NOT EXISTS public.forward_backtest_results (
        as_of_date date NOT NULL,
        ticker varchar(20) NOT NULL,
        horizon_days int4 NOT NULL,
        strategy text NOT NULL,
        model_name varchar(150) NOT NULL,
        signal varchar(10) NOT NULL,
        realized_return float8 NOT NULL,
        gross_strategy_return float8 NOT NULL,
        transaction_cost float8 NOT NULL,
        net_strategy_return float8 NOT NULL,
        created_at timestamptz DEFAULT now() NOT NULL,
        CONSTRAINT forward_backtest_results_pk PRIMARY KEY
            (as_of_date, ticker, horizon_days, strategy, model_name)
    );
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_forward_backtest_results_lookup
    ON public.forward_backtest_results
      (horizon_days, strategy, as_of_date DESC, model_name);
    """,
    """
    CREATE OR REPLACE VIEW public.forward_backtest_summary AS
    SELECT
      split_part(model_name, '@', 1) AS model_family,
      horizon_days,
      strategy,
      COUNT(*) AS observations,
      COUNT(*) FILTER (WHERE signal IN ('BUY', 'SELL')) AS trades,
      AVG(net_strategy_return) AS average_net_return,
      SUM(net_strategy_return) AS total_net_return,
      AVG(CASE WHEN signal IN ('BUY', 'SELL') THEN (net_strategy_return > 0)::int END)::float8 AS trade_win_rate,
      MIN(as_of_date) AS first_date,
      MAX(as_of_date) AS last_date
    FROM public.forward_backtest_results
    GROUP BY split_part(model_name, '@', 1), horizon_days, strategy;
    """,
    # -------------------------------------------------------------------------
    # ✅ ADDITIONS YOU REQUESTED
    # -------------------------------------------------------------------------

    # ---------------- daily_signal_features ----------------
    """
    CREATE TABLE IF NOT EXISTS public.daily_signal_features (
        as_of_date date NOT NULL,
        ticker varchar(20) NOT NULL,
        horizon_days int4 NOT NULL,
        close_price numeric(18, 6) NULL,
        vwap numeric(18, 6) NULL,
        rsi_14 numeric(18, 6) NULL,
        macd_hist numeric(18, 6) NULL,
        volatility_30d numeric(18, 6) NULL,
        volume_zscore_30 numeric(18, 6) NULL,
        sma_50 numeric(18, 6) NULL,
        created_at timestamptz DEFAULT now() NOT NULL,
        CONSTRAINT daily_signal_features_pk PRIMARY KEY (as_of_date, ticker, horizon_days)
    );
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_daily_signal_features_date
    ON public.daily_signal_features USING btree (as_of_date DESC, horizon_days);
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_daily_signal_features_ticker_date
    ON public.daily_signal_features USING btree (ticker, as_of_date DESC);
    """,

    # ---------------- model_eval_metrics_daily ----------------
    """
    CREATE TABLE IF NOT EXISTS public.model_eval_metrics_daily (
        as_of_date date NOT NULL,
        horizon_days int4 NOT NULL,
        model_name text NOT NULL,
        strategy text NOT NULL,
        n_rows int4 NOT NULL,
        positive_rate float8 NULL,
        auc float8 NULL,
        logloss float8 NULL,
        brier float8 NULL,
        ece float8 NULL,
        calib_bins_json jsonb NULL,
        tn int4 NULL,
        fp int4 NULL,
        fn int4 NULL,
        tp int4 NULL,
        threshold float8 NULL,
        pnl_avg float8 NULL,
        pnl_sum float8 NULL,
        n_buys int4 NULL,
        n_sells int4 NULL,
        n_holds int4 NULL,
        created_at timestamptz DEFAULT now() NOT NULL,
        CONSTRAINT model_eval_metrics_daily_pkey PRIMARY KEY (as_of_date, horizon_days, model_name, strategy)
    );
    """,

    # label_basis records WHAT the model was graded against, so a barrier model's
    # AUC is never silently compared to a forward-return model's AUC on a
    # different target. source separates production from research rows.
    """
    ALTER TABLE public.model_eval_metrics_daily
        ADD COLUMN IF NOT EXISTS label_basis text NULL,
        ADD COLUMN IF NOT EXISTS source text NULL;
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_model_eval_metrics_basis
    ON public.model_eval_metrics_daily USING btree (label_basis, horizon_days, as_of_date DESC);
    """,

    # ---------------- legacy signal backfill ----------------
    # daily_trade_signals.model_name was added later, so pre-existing rows default
    # to 'unknown' and would never join to model_predictions in the eval DAG.
    # Map legacy strategy names back to the model that produced them. Idempotent:
    # only touches rows still marked 'unknown'.
    """
    UPDATE public.daily_trade_signals
    SET model_name = 'xgb_dual_v2'
    WHERE model_name = 'unknown'
      AND strategy IN ('xgb_prob_vol_v2', 'xgb_top10_buy_v2', 'xgb_top10_sell_v2');
    """,

    # ---------------- model_registry ----------------
    """
    CREATE TABLE IF NOT EXISTS public.model_registry (
        model_name text NOT NULL,
        horizon_days int4 NOT NULL,
        feature_table text NOT NULL,
        trained_for_date date NOT NULL,
        metrics_json jsonb NULL,
        model_path text NOT NULL,
        created_at timestamptz DEFAULT now() NOT NULL,
        CONSTRAINT model_registry_pkey PRIMARY KEY (model_name, horizon_days, trained_for_date)
    );
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_model_registry_lookup
    ON public.model_registry USING btree (model_name, horizon_days, trained_for_date DESC);
    """,
    # Registry must fully describe a model artifact so that a triple-barrier model
    # can never be confused with (or overwrite) a forward-return model.
    """
    ALTER TABLE public.model_registry
        ADD COLUMN IF NOT EXISTS strategy text NULL,
        ADD COLUMN IF NOT EXISTS label_version text NULL,
        ADD COLUMN IF NOT EXISTS feature_version text NULL,
        ADD COLUMN IF NOT EXISTS feature_cols_json jsonb NULL,
        ADD COLUMN IF NOT EXISTS train_start_date date NULL,
        ADD COLUMN IF NOT EXISTS train_end_date date NULL,
        ADD COLUMN IF NOT EXISTS train_rows int4 NULL,
        ADD COLUMN IF NOT EXISTS run_mode text NULL;
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_model_registry_strategy
    ON public.model_registry USING btree (strategy, horizon_days, trained_for_date DESC);
    """,

    # ---------------- ticker_strategy_pnl ----------------
    """
    CREATE TABLE IF NOT EXISTS public.ticker_strategy_pnl (
        as_of_date date NOT NULL,
        ticker text NOT NULL,
        horizon_days int4 NOT NULL,
        model_name text NOT NULL,
        strategy text NOT NULL,
        signal text NOT NULL,
        entry_date date NOT NULL,
        exit_date date NOT NULL,
        entry_px float8 NOT NULL,
        exit_px float8 NOT NULL,
        ret float8 NOT NULL,
        pnl float8 NOT NULL,
        created_at timestamptz DEFAULT now() NOT NULL,
        CONSTRAINT ticker_strategy_pnl_pk PRIMARY KEY
            (as_of_date, ticker, horizon_days, strategy, model_name)
    );
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_ticker_strategy_pnl_asof
    ON public.ticker_strategy_pnl USING btree (as_of_date, horizon_days, strategy);
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_ticker_strategy_pnl_lookup
    ON public.ticker_strategy_pnl USING btree (ticker, horizon_days, strategy, as_of_date);
    """,

    # ---------------- rule_based_backtest_results ----------------
    """
    CREATE TABLE IF NOT EXISTS public.rule_based_backtest_results (
        run_name text NOT NULL,
        entry_signal_date date NOT NULL,
        ticker varchar(20) NOT NULL,
        horizon_days int4 NOT NULL,
        entry_rule text NOT NULL,
        exit_policy text NOT NULL,
        entry_date date NOT NULL,
        exit_date date NOT NULL,
        entry_price float8 NOT NULL,
        exit_price float8 NOT NULL,
        gross_return float8 NOT NULL,
        transaction_cost float8 NOT NULL,
        net_return float8 NOT NULL,
        exit_reason text NOT NULL,
        holding_days int4 NOT NULL,
        created_at timestamptz NOT NULL DEFAULT now(),
        CONSTRAINT rule_based_backtest_results_pk PRIMARY KEY
            (run_name, entry_signal_date, ticker, horizon_days, entry_rule, exit_policy)
    );
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_rule_backtest_lookup
    ON public.rule_based_backtest_results
        (run_name, horizon_days, entry_rule, exit_policy, entry_signal_date);
    """,
    """
    CREATE OR REPLACE VIEW public.rule_based_backtest_summary AS
    SELECT
        run_name,
        horizon_days,
        entry_rule,
        exit_policy,
        COUNT(*) AS trades,
        AVG(net_return) AS average_net_return,
        percentile_cont(0.5) WITHIN GROUP (ORDER BY net_return) AS median_net_return,
        AVG((net_return > 0)::int)::float8 AS win_rate,
        STDDEV_SAMP(net_return) AS return_stddev,
        MIN(net_return) AS worst_trade,
        MAX(net_return) AS best_trade,
        AVG(holding_days) AS average_holding_days,
        MIN(entry_signal_date) AS first_signal_date,
        MAX(entry_signal_date) AS last_signal_date
    FROM public.rule_based_backtest_results
    GROUP BY run_name, horizon_days, entry_rule, exit_policy;
    """,

    # ---------------- portfolio_backtest ----------------
    """
    CREATE TABLE IF NOT EXISTS public.portfolio_backtest_runs (
        run_name text PRIMARY KEY,
        model_family text NOT NULL,
        strategy text NOT NULL,
        horizon_days int4 NOT NULL,
        initial_capital float8 NOT NULL,
        final_equity float8 NOT NULL,
        total_return float8 NOT NULL,
        cagr float8 NULL,
        annualized_volatility float8 NULL,
        sharpe_ratio float8 NULL,
        max_drawdown float8 NULL,
        average_exposure float8 NULL,
        max_positions_used int4 NOT NULL,
        trades int4 NOT NULL,
        win_rate float8 NULL,
        average_trade_return float8 NULL,
        median_trade_return float8 NULL,
        spy_return float8 NULL,
        qqq_return float8 NULL,
        excess_return_spy float8 NULL,
        excess_return_qqq float8 NULL,
        config_json jsonb NOT NULL,
        first_date date NOT NULL,
        last_date date NOT NULL,
        created_at timestamptz NOT NULL DEFAULT now()
    );
    """,
    """
    ALTER TABLE public.portfolio_backtest_runs
        ADD COLUMN IF NOT EXISTS average_exposure float8 NULL,
        ADD COLUMN IF NOT EXISTS max_positions_used int4 NOT NULL DEFAULT 0;
    """,
    """
    CREATE TABLE IF NOT EXISTS public.portfolio_backtest_daily (
        run_name text NOT NULL,
        trade_date date NOT NULL,
        cash float8 NOT NULL,
        market_value float8 NOT NULL,
        equity float8 NOT NULL,
        daily_return float8 NULL,
        drawdown float8 NOT NULL,
        open_positions int4 NOT NULL,
        entries int4 NOT NULL,
        exits int4 NOT NULL,
        spy_equity float8 NULL,
        qqq_equity float8 NULL,
        CONSTRAINT portfolio_backtest_daily_pk PRIMARY KEY (run_name, trade_date),
        CONSTRAINT portfolio_backtest_daily_run_fk FOREIGN KEY (run_name)
            REFERENCES public.portfolio_backtest_runs(run_name) ON DELETE CASCADE
    );
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_portfolio_backtest_daily_date
    ON public.portfolio_backtest_daily (run_name, trade_date);
    """,
    """
    CREATE TABLE IF NOT EXISTS public.portfolio_backtest_trades (
        run_name text NOT NULL,
        ticker varchar(20) NOT NULL,
        signal_date date NOT NULL,
        entry_date date NOT NULL,
        exit_date date NOT NULL,
        horizon_days int4 NOT NULL,
        score float8 NULL,
        shares float8 NOT NULL,
        entry_price float8 NOT NULL,
        exit_price float8 NOT NULL,
        entry_value float8 NOT NULL,
        entry_cost float8 NOT NULL,
        exit_value float8 NOT NULL,
        exit_cost float8 NOT NULL,
        net_pnl float8 NOT NULL,
        net_return float8 NOT NULL,
        holding_days int4 NOT NULL,
        exit_reason text NOT NULL,
        CONSTRAINT portfolio_backtest_trades_pk
            PRIMARY KEY (run_name, ticker, entry_date),
        CONSTRAINT portfolio_backtest_trades_run_fk FOREIGN KEY (run_name)
            REFERENCES public.portfolio_backtest_runs(run_name) ON DELETE CASCADE
    );
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_portfolio_backtest_trades_lookup
    ON public.portfolio_backtest_trades (run_name, exit_date, ticker);
    """,
    """
    DO $$
    DECLARE
      current_pk text;
      current_pk_def text;
    BEGIN
      SELECT c.conname, pg_get_constraintdef(c.oid)
        INTO current_pk, current_pk_def
      FROM pg_constraint c
      WHERE c.conrelid = 'public.ticker_strategy_pnl'::regclass
        AND c.contype = 'p';
      IF current_pk_def IS NULL OR current_pk_def NOT LIKE '%model_name%' THEN
        IF current_pk IS NOT NULL THEN
          EXECUTE format('ALTER TABLE public.ticker_strategy_pnl DROP CONSTRAINT %I', current_pk);
        END IF;
        ALTER TABLE public.ticker_strategy_pnl
          ADD CONSTRAINT ticker_strategy_pnl_pk PRIMARY KEY
          (as_of_date, ticker, horizon_days, strategy, model_name);
      END IF;
    END $$;
    """,
]

def run_schema_bootstrap(**_):
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = False
    try:
        with conn.cursor() as cur:
            for i, stmt in enumerate(DDL_STATEMENTS, start=1):
                logger.info("Executing DDL %d/%d", i, len(DDL_STATEMENTS))
                cur.execute(stmt)
        conn.commit()
        logger.info("Schema bootstrap completed successfully.")
    except Exception:
        conn.rollback()
        logger.exception("Schema bootstrap failed; rolled back.")
        raise
    finally:
        conn.close()

default_args = {"owner": "airflow", "retries": 1, "retry_delay": timedelta(minutes=5)}

with DAG(
    dag_id="db_schema_bootstrap",
    start_date=pendulum.datetime(2025, 12, 1, tz=LOCAL_TZ),
    schedule=None,            # run manually when needed
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["production-support", "db", "schema", "bootstrap"],
) as dag:

    PythonOperator(
        task_id="create_tables_and_indexes",
        python_callable=run_schema_bootstrap,
    )
