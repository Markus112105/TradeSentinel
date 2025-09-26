from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from tradesentinel import indicators


def test_ema_matches_manual_average() -> None:
    prices = pd.Series([10, 11, 12, 13, 14, 15], dtype=float)
    result = indicators.ema(prices, span=3)
    assert result.notna().sum() == len(prices)
    # EMA should start at the first value for adjust=False configuration.
    assert result.iloc[0] == pytest.approx(prices.iloc[0])
    assert result.iloc[-1] > result.iloc[0]


def test_rsi_stays_within_bounds() -> None:
    prices = pd.Series(np.linspace(100, 110, num=50), dtype=float)
    result = indicators.rsi(prices, period=14)
    assert result.between(0, 100).all()


def test_bollinger_bands_width_positive() -> None:
    prices = pd.Series(np.linspace(100, 120, num=40), dtype=float)
    upper, lower = indicators.bollinger_bands(prices, window=10, num_std=2.0)
    assert (upper - lower).dropna().gt(0).all()


def test_historical_volatility_zero_for_constant_prices() -> None:
    prices = pd.Series([100] * 40, dtype=float)
    vol = indicators.historical_volatility(prices, window=5)
    assert vol.dropna().eq(0).all()


def test_indicator_input_validation() -> None:
    empty = pd.Series([], dtype=float)
    with pytest.raises(indicators.IndicatorCalculationError):
        indicators.ema(empty, span=3)
