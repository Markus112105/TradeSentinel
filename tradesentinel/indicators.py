"""Technical indicator calculations built on top of pandas Series operations.

This module centralizes EMA, RSI, rolling volatility, and Bollinger Band
computations. All functions accept pandas Series inputs to leverage vectorized
operations, keeping complexity linear in the number of observations while
maintaining readability. Dependencies: `pandas`, `numpy`.
"""
from __future__ import annotations

from typing import Tuple

import numpy as np
import pandas as pd


class IndicatorCalculationError(ValueError):
    """Raised when indicator inputs are invalid or insufficient."""


def _validate_price_series(series: pd.Series, min_length: int, label: str) -> None:
    """Ensure the series has enough non-null observations for the indicator."""

    if series is None or series.empty:
        raise IndicatorCalculationError(f"{label} series is empty")
    if series.isna().all():
        raise IndicatorCalculationError(f"{label} series contains only NaN values")
    if series.dropna().shape[0] < min_length:
        raise IndicatorCalculationError(
            f"{label} series requires at least {min_length} valid points"
        )


def ema(series: pd.Series, span: int) -> pd.Series:
    """Compute an exponential moving average using pandas ewm smoothing.

    Complexity: O(n) because pandas maintains the recurrence with constant-time
    updates per row.
    """

    if span <= 1:
        raise IndicatorCalculationError("EMA span must be greater than 1")
    _validate_price_series(series, min_length=span, label="Price")
    return series.ewm(span=span, adjust=False).mean()


def rsi(series: pd.Series, period: int = 14) -> pd.Series:
    """Calculate Relative Strength Index using Wilder's smoothing.

    Complexity: O(n). Uses pandas diff to compute vectorized gain/loss arrays,
    avoiding explicit Python loops.
    """

    if period <= 1:
        raise IndicatorCalculationError("RSI period must be greater than 1")
    _validate_price_series(series, min_length=period + 1, label="Price")

    delta = series.diff()
    gains = delta.clip(lower=0)
    losses = -delta.clip(upper=0)
    avg_gain = gains.ewm(alpha=1 / period, adjust=False).mean()
    avg_loss = losses.ewm(alpha=1 / period, adjust=False).mean()

    rs = avg_gain / avg_loss.replace(to_replace=0, value=np.nan)
    rsi_series = 100 - (100 / (1 + rs))
    return rsi_series.fillna(0)


def bollinger_bands(
    series: pd.Series, window: int = 20, num_std: float = 2.0
) -> Tuple[pd.Series, pd.Series]:
    """Return upper and lower Bollinger Bands.

    Complexity: O(n) by relying on pandas rolling aggregations.
    """

    if window <= 1:
        raise IndicatorCalculationError("Bollinger window must be greater than 1")
    _validate_price_series(series, min_length=window, label="Price")

    rolling = series.rolling(window=window, min_periods=window)
    mean = rolling.mean()
    std = rolling.std()
    upper = mean + num_std * std
    lower = mean - num_std * std
    return upper, lower


def historical_volatility(series: pd.Series, window: int = 20, trading_days: int = 252) -> pd.Series:
    """Annualized rolling volatility derived from log returns.

    Complexity: O(n) because we compute log returns vectorized and use rolling
    std deviation. The annualization scales volatility for comparability.
    """

    if window <= 1:
        raise IndicatorCalculationError("Volatility window must be greater than 1")
    _validate_price_series(series, min_length=window + 1, label="Price")

    log_returns = np.log(series / series.shift(1))
    rolling_std = log_returns.rolling(window=window, min_periods=window).std()
    return rolling_std * np.sqrt(trading_days)


__all__ = [
    "IndicatorCalculationError",
    "ema",
    "rsi",
    "bollinger_bands",
    "historical_volatility",
]
