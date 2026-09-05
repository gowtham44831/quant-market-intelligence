-- XGBoost storage schema used by the forward-return and triple-barrier DAGs.
-- This file intentionally contains DDL only; it is not executed by a DAG.
-- If recreating tables, drop dependent objects/data explicitly before running it.

CREATE TABLE public.model_registry (
    model_name text NOT NULL,
    horizon_days int4 NOT NULL,
    feature_table text NOT NULL,
    trained_for_date date NOT NULL,
    metrics_json jsonb NULL,
    model_path text NOT NULL,
    created_at timestamptz DEFAULT now() NOT NULL,
    strategy text NULL,
    label_version text NULL,
    feature_version text NULL,
    feature_cols_json jsonb NULL,
    train_start_date date NULL,
    train_end_date date NULL,
    train_rows int4 NULL,
    run_mode text NULL,
    CONSTRAINT model_registry_pkey
        PRIMARY KEY (model_name, horizon_days, trained_for_date)
);

CREATE INDEX idx_model_registry_lookup
    ON public.model_registry (model_name, horizon_days, trained_for_date DESC);
CREATE INDEX idx_model_registry_strategy
    ON public.model_registry (strategy, horizon_days, trained_for_date DESC);


CREATE TABLE public.model_predictions (
    as_of_date date NOT NULL,
    horizon_days int4 NOT NULL,
    ticker varchar(20) NOT NULL,
    prob_up numeric(10, 6) NOT NULL,
    prob_down numeric(10, 6) NULL,
    score numeric(12, 6) NOT NULL,
    model_name text NOT NULL,
    created_at timestamptz DEFAULT now() NOT NULL,
    CONSTRAINT model_predictions_pk
        PRIMARY KEY (as_of_date, horizon_days, ticker, model_name)
);

CREATE INDEX idx_model_predictions_date_horizon
    ON public.model_predictions (as_of_date DESC, horizon_days);
CREATE INDEX idx_model_predictions_ticker_date
    ON public.model_predictions (ticker, as_of_date DESC);


CREATE TABLE public.daily_trade_signals (
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
    CONSTRAINT daily_trade_signals_pk
        PRIMARY KEY (as_of_date, ticker, horizon_days, strategy)
);

CREATE INDEX idx_trade_signals_date_strategy
    ON public.daily_trade_signals (as_of_date DESC, strategy, horizon_days);
CREATE INDEX idx_trade_signals_model_name
    ON public.daily_trade_signals (model_name, as_of_date DESC);
CREATE INDEX idx_trade_signals_signal_date
    ON public.daily_trade_signals (signal, as_of_date DESC);


CREATE TABLE public.backtest_trade_signals (
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

CREATE INDEX idx_backtest_signals_date_strategy
    ON public.backtest_trade_signals (as_of_date DESC, strategy, horizon_days);
CREATE INDEX idx_backtest_signals_model
    ON public.backtest_trade_signals (model_name, horizon_days, as_of_date DESC);
CREATE INDEX idx_backtest_signals_signal_date
    ON public.backtest_trade_signals (signal, as_of_date DESC);


CREATE TABLE public.model_eval_metrics_daily (
    as_of_date date NOT NULL,
    horizon_days int4 NOT NULL,
    model_name text NOT NULL,
    strategy text NOT NULL,
    label_basis text NULL,
    source text NULL,
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
    CONSTRAINT model_eval_metrics_daily_pkey
        PRIMARY KEY (as_of_date, horizon_days, model_name, strategy)
);

CREATE INDEX idx_model_eval_metrics_basis
    ON public.model_eval_metrics_daily (label_basis, horizon_days, as_of_date DESC);


CREATE TABLE public.forward_backtest_results (
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
    CONSTRAINT forward_backtest_results_pk
        PRIMARY KEY (as_of_date, ticker, horizon_days, strategy, model_name)
);

CREATE INDEX idx_forward_backtest_results_lookup
    ON public.forward_backtest_results
       (horizon_days, strategy, as_of_date DESC, model_name);

CREATE VIEW public.forward_backtest_summary AS
SELECT
    split_part(model_name, '@', 1) AS model_family,
    horizon_days,
    strategy,
    COUNT(*) AS observations,
    COUNT(*) FILTER (WHERE signal IN ('BUY', 'SELL')) AS trades,
    AVG(net_strategy_return) AS average_net_return,
    SUM(net_strategy_return) AS total_net_return,
    AVG(
        CASE WHEN signal IN ('BUY', 'SELL')
             THEN (net_strategy_return > 0)::int END
    )::float8 AS trade_win_rate,
    MIN(as_of_date) AS first_date,
    MAX(as_of_date) AS last_date
FROM public.forward_backtest_results
GROUP BY split_part(model_name, '@', 1), horizon_days, strategy;


CREATE TABLE public.ticker_strategy_pnl (
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
    CONSTRAINT ticker_strategy_pnl_pk
        PRIMARY KEY (as_of_date, ticker, horizon_days, strategy, model_name)
);

CREATE INDEX idx_ticker_strategy_pnl_asof
    ON public.ticker_strategy_pnl (as_of_date, horizon_days, strategy);
CREATE INDEX idx_ticker_strategy_pnl_lookup
    ON public.ticker_strategy_pnl (ticker, horizon_days, strategy, as_of_date);
