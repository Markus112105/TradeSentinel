"""Streamlit dashboard for visualizing trading strategy performance.

This module orchestrates user input, market data retrieval, indicator and
backtest execution, and visualization. It relies on `streamlit` for UI,
`plotly` for charting, and the local ingestion/backtesting modules to keep the
business logic testable outside the dashboard runtime.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Tuple

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from tradesentinel.backtester import Backtester, StrategyConfig
from tradesentinel.data_ingestion import (
    DataIngestionError,
    IngestionConfig,
    MarketDataIngestor,
    default_ingestor,
)


_SAFE_DAILY_LOOKBACK = timedelta(days=365)
_SAFE_INTRADAY_LOOKBACK = timedelta(days=30)


def _resolve_ingestor(data_dir: Path | None) -> MarketDataIngestor:
    """Factory wrapper to support dependency injection for testing."""

    if data_dir is None:
        return default_ingestor()
    return MarketDataIngestor(config=IngestionConfig(data_dir=data_dir))


def _default_lookback(interval: str) -> timedelta:
    """Return a safe lookback to stay within common provider retention windows."""

    return _SAFE_DAILY_LOOKBACK if interval == "1d" else _SAFE_INTRADAY_LOOKBACK


def _sanitize_window(start: datetime, end: datetime, interval: str) -> tuple[datetime, datetime]:
    """Clamp user supplied dates to prevent future ranges or zero-width windows."""

    now = datetime.utcnow()
    upper = min(end, now)
    lower = start
    if lower >= upper:
        lookback = _default_lookback(interval)
        lower = upper - lookback
    return lower, upper


def _fallback_history(ticker: str, interval: str) -> pd.DataFrame:
    """Download a minimal but reliable history when the primary loader fails."""

    lookback = _default_lookback(interval)
    fallback_end = datetime.utcnow()
    fallback_start = fallback_end - lookback
    ingestor = default_ingestor()
    try:
        frame = ingestor.fetch_price_history(
            ticker=ticker,
            start=fallback_start,
            end=fallback_end,
            interval=interval,
        )
    except DataIngestionError:
        if interval != "1d":
            frame = ingestor.fetch_price_history(
                ticker=ticker,
                start=fallback_end - _SAFE_DAILY_LOOKBACK,
                end=fallback_end,
                interval="1d",
            )
        else:
            raise
    return frame


@st.cache_data(show_spinner=False)
def _load_history(
    ticker: str,
    start: datetime,
    end: datetime,
    interval: str,
    data_dir: str | None,
) -> pd.DataFrame:
    """Pull price history for the dashboard with inline error propagation."""

    normalized_start, normalized_end = _sanitize_window(start, end, interval)
    ingestor = _resolve_ingestor(Path(data_dir) if data_dir else None)
    # Returning a DataFrame keeps caching O(1) to rehydrate compared to storing tuples.
    try:
        frame = ingestor.fetch_price_history(
            ticker=ticker, start=normalized_start, end=normalized_end, interval=interval
        )
    except DataIngestionError as exc:
        frame = _fallback_history(ticker, interval)
        if frame.empty:
            raise DataIngestionError(
                "Fallback ingestion also returned no data; check network connectivity or provider availability."
            ) from exc
        return frame

    if frame.empty:
        frame = _fallback_history(ticker, interval)
        if frame.empty:
            raise DataIngestionError(
                "Fallback ingestion returned no data; adjust the date range or interval."
            )
    return frame


def _prepare_charts(orders: pd.DataFrame, equity: pd.Series) -> Tuple[go.Figure, go.Figure]:
    """Build price and equity figures using plotly for interactive inspection."""

    price_fig = go.Figure()
    price_fig.add_trace(
        go.Scatter(x=orders.index, y=orders["price"], mode="lines", name="Close Price")
    )
    price_fig.add_trace(
        go.Scatter(
            x=orders.index,
            y=orders["signal"],
            mode="lines",
            name="Signal",
            yaxis="y2",
            line=dict(dash="dash"),
        )
    )
    price_fig.update_layout(
        title="Price & Signals",
        yaxis=dict(title="Price"),
        yaxis2=dict(title="Signal", overlaying="y", side="right", range=[-0.1, 1.1]),
        legend=dict(orientation="h"),
    )

    equity_fig = go.Figure()
    equity_fig.add_trace(
        go.Scatter(x=equity.index, y=equity.values, mode="lines", name="Equity Curve")
    )
    equity_fig.update_layout(title="Equity Curve", yaxis=dict(title="Portfolio Value"))

    return price_fig, equity_fig


def _strategy_config_from_inputs(
    *,
    initial_cash: float,
    fast_window: int,
    slow_window: int,
    volatility_window: int,
    volatility_cap: float,
    transaction_cost: float,
) -> StrategyConfig:
    """Normalize sidebar parameters into a validated strategy configuration."""

    slow_clamped = max(int(slow_window), int(fast_window) + 1)
    config = StrategyConfig(
        initial_cash=float(initial_cash),
        fast_window=int(fast_window),
        slow_window=slow_clamped,
        volatility_window=int(volatility_window),
        volatility_cap=float(volatility_cap),
        transaction_cost=float(transaction_cost),
    )
    config.validate()
    return config


def render_dashboard(data_dir: Path | None = None) -> None:
    """Entry point for streamlit `run tradesentinel/dashboard.py`."""

    st.set_page_config(page_title="TradeSentinel", layout="wide")
    st.title("TradeSentinel — Volatility Strategy Dashboard")

    default_start = date(2020, 1, 1)
    default_end = date.today()

    ticker = st.text_input("Ticker", value="AAPL")
    start_date = st.date_input("Start", value=default_start)
    end_date = st.date_input("End", value=default_end)
    interval = st.selectbox("Interval", options=["1d", "1h", "30m", "15m"], index=0)

    with st.sidebar:
        st.header("Strategy Parameters")
        initial_cash = st.number_input(
            "Initial Cash",
            min_value=10_000.0,
            max_value=5_000_000.0,
            value=100_000.0,
            step=5_000.0,
            help="Higher capital magnifies position sizing while keeping percentage returns unchanged.",
        )
        fast_window = st.slider(
            "Fast EMA Window",
            min_value=3,
            max_value=100,
            value=21,
            help="Shorter window reacts faster but risks whipsaws.",
        )
        slow_default = max(fast_window + 1, 55)
        slow_window = st.slider(
            "Slow EMA Window",
            min_value=fast_window + 1,
            max_value=200,
            value=slow_default,
            help="Must exceed the fast window to maintain a clear crossover regime.",
        )
        volatility_window = st.slider(
            "Volatility Lookback",
            min_value=5,
            max_value=120,
            value=21,
            help="Controls the stability of the risk filter (O(n) rolling std).",
        )
        volatility_cap = st.slider(
            "Volatility Cap",
            min_value=0.05,
            max_value=1.0,
            value=0.40,
            step=0.05,
            help="Positions are suppressed when annualized vol exceeds this threshold.",
        )
        transaction_cost = st.slider(
            "Transaction Cost",
            min_value=0.0,
            max_value=0.01,
            value=0.0005,
            step=0.0005,
            help="Round-trip slippage assumption applied to turnover in O(n).",
        )

    config = _strategy_config_from_inputs(
        initial_cash=initial_cash,
        fast_window=fast_window,
        slow_window=slow_window,
        volatility_window=volatility_window,
        volatility_cap=volatility_cap,
        transaction_cost=transaction_cost,
    )
    backtester = Backtester(config=config)

    if start_date >= end_date:
        st.warning("Start date must be earlier than end date.")
        return

    if st.button("Run Backtest"):
        with st.spinner("Fetching data and running backtest..."):
            try:
                history = _load_history(
                    ticker=ticker,
                    start=datetime.combine(start_date, datetime.min.time()),
                    end=datetime.combine(end_date, datetime.min.time()),
                    interval=interval,
                    data_dir=str(data_dir) if data_dir else None,
                )
            except DataIngestionError as exc:
                st.error(f"Data ingestion failed: {exc}")
                return

            result = backtester.run(history)
            price_fig, equity_fig = _prepare_charts(
                orders=result.orders, equity=result.equity_curve
            )

            metrics = result.metrics
            st.subheader("Performance Metrics")
            st.caption(
                "Metrics reflect the parameterization shown in the sidebar; risk controls mutate position size multiplicatively."
            )
            for key, value in metrics.items():
                if any(token in key for token in ("return", "drawdown", "volatility")):
                    display_value = f"{value:.2%}"
                else:
                    display_value = f"{value:.2f}"
                st.metric(label=key.replace("_", " ").title(), value=display_value)

            col1, col2 = st.columns(2)
            with col1:
                st.plotly_chart(price_fig, use_container_width=True)
            with col2:
                st.plotly_chart(equity_fig, use_container_width=True)

            st.subheader("Order Log")
            # DataFrame keeps O(1) column access for ad-hoc investigation.
            st.dataframe(result.orders.tail(100))


def main() -> None:
    """CLI entry point when executed via `python -m tradesentinel.dashboard`."""

    render_dashboard()


if __name__ == "__main__":  # pragma: no cover - UI script entry point
    main()
