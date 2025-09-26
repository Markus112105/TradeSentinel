"""Vectorized backtesting engine for deterministic strategy evaluation.

This module wires market data and indicator calculations into a reproducible
strategy simulator. It favors pandas DataFrames/Series to keep the main loops
vectorized (O(n)) and documentable for production review. Dependencies:
`pandas`, `numpy`, and the local `indicators` module.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict

import numpy as np
import pandas as pd

from tradesentinel import indicators


class BacktestError(RuntimeError):
    """Raised when inputs prevent a deterministic backtest run."""


@dataclass(frozen=True)
class StrategyConfig:
    """Parameter set controlling the default trend-following strategy."""

    initial_cash: float = 100_000.0
    fast_window: int = 21
    slow_window: int = 55
    volatility_window: int = 21
    volatility_cap: float = 0.40
    transaction_cost: float = 0.0005

    def validate(self) -> None:
        """Sanity-check configuration values before running a backtest."""

        if self.fast_window <= 1 or self.slow_window <= 1:
            raise BacktestError("EMA windows must be greater than 1")
        if self.fast_window >= self.slow_window:
            raise BacktestError("Fast window must be strictly less than slow window")
        if self.volatility_window <= 1:
            raise BacktestError("Volatility window must be greater than 1")
        if not (0 <= self.transaction_cost < 0.01):
            raise BacktestError("Transaction cost must be in [0, 0.01)")
        if not (0 < self.volatility_cap < 2):
            raise BacktestError("Volatility cap must be between 0 and 2 (annualized)")


@dataclass(frozen=True)
class BacktestResult:
    """Container for portfolio equity, order log, and summary metrics."""

    equity_curve: pd.Series
    orders: pd.DataFrame
    metrics: Dict[str, float]


class Backtester:
    """Coordinate signal generation and portfolio simulation."""

    def __init__(self, config: StrategyConfig | None = None) -> None:
        self._config = config or StrategyConfig()
        self._config.validate()

    def run(self, price_frame: pd.DataFrame) -> BacktestResult:
        """Execute the configured strategy on the supplied OHLCV data."""

        if "close" not in price_frame.columns:
            raise BacktestError("Price frame must include a 'close' column")
        if not isinstance(price_frame.index, pd.DatetimeIndex):
            raise BacktestError("Price frame index must be a DatetimeIndex")
        if price_frame.shape[0] < self._config.slow_window + 5:
            raise BacktestError("Price frame length is insufficient for the slow EMA")

        close = price_frame["close"].copy()
        # Vectorized indicator calculations keep complexity linear in history length.
        fast = indicators.ema(close, span=self._config.fast_window)
        slow = indicators.ema(close, span=self._config.slow_window)
        vol = indicators.historical_volatility(
            close, window=self._config.volatility_window
        )

        signal = self._generate_signal(fast=fast, slow=slow, vol=vol)
        positions = signal.shift(1).fillna(0.0)
        daily_returns = close.pct_change().fillna(0.0)
        gross_returns = positions * daily_returns

        # Turnover uses diff on the vectorized position Series so cost-only events stay O(n).
        turnover = positions.diff().abs().fillna(positions.abs())
        trading_costs = turnover * self._config.transaction_cost

        net_returns = gross_returns - trading_costs
        equity_curve = (1 + net_returns).cumprod() * self._config.initial_cash

        # Orders log remains a pandas DataFrame for direct columnar inspection during debugging.
        orders = pd.DataFrame(
            {
                "signal": signal,
                "position": positions,
                "turnover": turnover,
                "price": close,
                "volatility": vol,
            }
        )

        metrics = self._compute_metrics(equity_curve=equity_curve, net_returns=net_returns)
        return BacktestResult(equity_curve=equity_curve, orders=orders, metrics=metrics)

    def _generate_signal(
        self, fast: pd.Series, slow: pd.Series, vol: pd.Series
    ) -> pd.Series:
        """Build a long/flat signal with volatility regime filter."""

        long_trend = fast > slow
        raw_signal = long_trend.astype(float)
        # Filtering via volatility cap prevents over-trading when risk is elevated.
        safe = vol <= self._config.volatility_cap
        signal = raw_signal.where(safe, other=0.0)
        return signal.fillna(0.0)

    def _compute_metrics(
        self, equity_curve: pd.Series, net_returns: pd.Series
    ) -> Dict[str, float]:
        """Calculate deterministic performance statistics."""

        total_return = equity_curve.iloc[-1] / equity_curve.iloc[0] - 1
        daily_mean = net_returns.mean()
        daily_std = net_returns.std(ddof=0)
        sharpe = 0.0 if daily_std == 0 else (daily_mean / daily_std) * np.sqrt(252)
        drawdown = equity_curve / equity_curve.cummax() - 1
        max_drawdown = drawdown.min()

        return {
            "total_return": float(total_return),
            "annualized_return": float((1 + daily_mean) ** 252 - 1),
            "annualized_volatility": float(daily_std * np.sqrt(252)),
            "sharpe_ratio": float(sharpe),
            "max_drawdown": float(max_drawdown),
        }


__all__ = [
    "BacktestError",
    "Backtester",
    "BacktestResult",
    "StrategyConfig",
]
