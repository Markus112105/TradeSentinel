from __future__ import annotations

import pandas as pd
import pytest

from tradesentinel import dashboard
from tradesentinel.backtester import BacktestError
from tradesentinel.data_ingestion import MarketDataIngestor


def test_prepare_charts_has_expected_traces() -> None:
    index = pd.date_range("2024-01-01", periods=5, freq="D")
    orders = pd.DataFrame(
        {
            "signal": [0, 1, 1, 0, 1],
            "position": [0, 0, 1, 1, 0],
            "turnover": [0, 1, 0, 1, 1],
            "price": [100, 101, 102, 101, 103],
            "volatility": [0.1] * 5,
        },
        index=index,
    )
    equity = pd.Series([10_000, 10_100, 10_300, 10_150, 10_500], index=index, dtype=float)

    price_fig, equity_fig = dashboard._prepare_charts(orders, equity)

    assert len(price_fig.data) == 2
    assert len(equity_fig.data) == 1


def test_resolve_ingestor_explicit_directory(tmp_path) -> None:
    ingestor = dashboard._resolve_ingestor(tmp_path)
    assert isinstance(ingestor, MarketDataIngestor)
    # Touching the protected member is acceptable here to confirm the test directory is honored.
    assert ingestor._data_dir == tmp_path


def test_strategy_config_from_inputs_clamps_slow_window() -> None:
    config = dashboard._strategy_config_from_inputs(
        initial_cash=50_000.0,
        fast_window=15,
        slow_window=10,
        volatility_window=20,
        volatility_cap=0.35,
        transaction_cost=0.001,
    )
    assert config.slow_window == 16
    assert config.fast_window == 15
    assert config.initial_cash == pytest.approx(50_000.0)


def test_strategy_config_from_inputs_respects_validation() -> None:
    with pytest.raises(BacktestError):
        dashboard._strategy_config_from_inputs(
            initial_cash=50_000.0,
            fast_window=1,
            slow_window=2,
            volatility_window=1,
            volatility_cap=0.0,
            transaction_cost=0.02,
        )
