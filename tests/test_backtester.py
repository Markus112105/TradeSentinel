from __future__ import annotations

from dataclasses import replace

import pandas as pd
import pytest

from tradesentinel.backtester import BacktestError, Backtester, StrategyConfig


def _price_frame() -> pd.DataFrame:
    index = pd.date_range("2020-01-01", periods=120, freq="D")
    base = pd.Series(range(120), index=index, dtype=float)
    data = {
        "open": 100 + base,
        "high": 101 + base,
        "low": 99 + base,
        "close": 100 + base,
        "adj close": 100 + base,
        "volume": pd.Series([1_000] * 120, index=index, dtype=float),
    }
    return pd.DataFrame(data, index=index)


def test_backtester_run_produces_metrics() -> None:
    frame = _price_frame()
    config = StrategyConfig(initial_cash=10_000.0, fast_window=5, slow_window=20, volatility_window=10)
    tester = Backtester(config=config)
    result = tester.run(frame)

    assert not result.orders.empty
    assert result.equity_curve.iloc[0] == pytest.approx(config.initial_cash)
    assert set(result.metrics).issuperset(
        {"total_return", "annualized_return", "sharpe_ratio", "max_drawdown"}
    )


def test_strategy_config_validation() -> None:
    config = StrategyConfig(fast_window=20, slow_window=10)
    with pytest.raises(BacktestError):
        Backtester(config=config)


def test_run_requires_close_column() -> None:
    tester = Backtester()
    frame = _price_frame().drop(columns=["close"])
    with pytest.raises(BacktestError):
        tester.run(frame)


def test_transaction_costs_reduce_returns() -> None:
    frame = _price_frame()
    base_config = StrategyConfig(initial_cash=10_000.0, fast_window=5, slow_window=20, volatility_window=10, transaction_cost=0.0)
    tester_no_cost = Backtester(config=base_config)
    result_no_cost = tester_no_cost.run(frame)

    costly_config = replace(base_config, transaction_cost=0.005)
    tester_with_cost = Backtester(config=costly_config)
    result_with_cost = tester_with_cost.run(frame)

    assert result_with_cost.equity_curve.iloc[-1] <= result_no_cost.equity_curve.iloc[-1]
