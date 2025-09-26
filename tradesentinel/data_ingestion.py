"""Data ingestion and caching utilities for the trading dashboard.

This module downloads historical price data from Yahoo Finance via yfinance,
implements deterministic filesystem caching for reproducibility, and exposes
an interface that decouples data acquisition from downstream indicator or
backtesting logic. It depends on `pandas`, `numpy`, `yfinance`, and standard
library modules only.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd
import yfinance as yf


EXPECTED_COLUMNS = ("adj close", "close", "high", "low", "open", "volume")


class DataIngestionError(RuntimeError):
    """Raised when market data cannot be ingested or validated."""


@dataclass(frozen=True)
class IngestionConfig:
    """Configuration for deterministic market data ingestion."""

    data_dir: Path
    auto_cache: bool = True
    min_rows: int = 10

    def resolve_data_dir(self) -> Path:
        """Return the data directory, creating it when necessary."""

        if not self.data_dir.exists():
            self.data_dir.mkdir(parents=True, exist_ok=True)
        return self.data_dir


@dataclass(frozen=True)
class NormalizedWindow:
    """Describe the adjusted download window after retention clamping."""

    start: datetime
    end: datetime
    truncated: bool
    retention_cutoff: Optional[datetime] = None


class MarketDataIngestor:
    """Download and cache Yahoo Finance price history for downstream modules."""

    def __init__(self, config: IngestionConfig) -> None:
        self._config = config
        self._data_dir = self._config.resolve_data_dir()
        # Yahoo Finance truncates intraday history to fixed retention windows; we encode
        # the limits here to ensure caching remains deterministic when we clamp ranges.
        self._intraday_lookback = {
            "1m": timedelta(days=7),
            "2m": timedelta(days=60),
            "5m": timedelta(days=60),
            "15m": timedelta(days=60),
            "30m": timedelta(days=60),
            "1h": timedelta(days=60),
            "60m": timedelta(days=60),
            "90m": timedelta(days=60),
        }

    def fetch_price_history(
        self,
        ticker: str,
        start: datetime,
        end: datetime,
        interval: str = "1d",
        force_refresh: bool = False,
    ) -> pd.DataFrame:
        """Fetch adjusted OHLCV data using cache-aware filesystem storage.

        Parameters
        ----------
        ticker:
            Symbol passed directly to Yahoo Finance; case-insensitive.
        start:
            Inclusive start of the requested history.
        end:
            Exclusive end of the requested history.
        interval:
            Sampling frequency such as "1d" or "1h". See yfinance docs.
        force_refresh:
            When True, ignore the cache and re-download the dataset.
        """

        window = self._normalize_window(start=start, end=end, interval=interval)
        if window.retention_cutoff is not None:
            lookback = self._intraday_lookback[interval]
            cutoff_date = window.retention_cutoff.date().isoformat()
            lookback_days = max(lookback.days, 1)
            raise DataIngestionError(
                "Requested intraday window predates Yahoo Finance's retention limit. "
                f"History before {cutoff_date} (~{lookback_days} days) is unavailable; "
                "move the end date forward or request the daily interval instead."
            )

        normalized_start, normalized_end, truncated = (
            window.start,
            window.end,
            window.truncated,
        )
        cache_path = self._cache_path(ticker, normalized_start, normalized_end, interval)
        legacy_cache_path = None
        if truncated:
            legacy_cache_path = self._cache_path(ticker, start, end, interval)

        if self._config.auto_cache and not force_refresh:
            for candidate in (cache_path, legacy_cache_path):
                if candidate and candidate.exists():
                    frame = self._load_from_cache(candidate)
                    if (
                        candidate is legacy_cache_path
                        and legacy_cache_path is not None
                        and not cache_path.exists()
                    ):
                        frame.to_parquet(cache_path, index=True)
                    return frame

        frame = self._download_from_yfinance(
            ticker, normalized_start, normalized_end, interval
        )
        if frame.empty:
            hint = (
                " Yahoo Finance restricts intraday history to roughly 60 days; "
                "shorten the date range or switch to the daily interval."
            )
            if truncated:
                raise DataIngestionError(
                    f"Received empty dataset for {ticker} after range normalization." + hint
                )
            raise DataIngestionError(f"Received empty dataset for {ticker}." + hint)

        self._validate_frame(frame, ticker)

        if self._config.auto_cache:
            # Parquet gives O(1) metadata lookup for subsequent loads via filename encoding.
            frame.to_parquet(cache_path, index=True)

        return frame

    def _download_from_yfinance(
        self, ticker: str, start: datetime, end: datetime, interval: str
    ) -> pd.DataFrame:
        """Download fresh data from Yahoo Finance with error handling wrappers."""

        try:
            ticker_client = yf.Ticker(ticker)
            frame = ticker_client.history(
                start=start,
                end=end,
                interval=interval,
                auto_adjust=False,
                actions=False,
                raise_errors=True,
            )
        except Exception as exc:  # pragma: no cover - defensive real-world safeguard
            raise DataIngestionError(f"Failed to download data for {ticker}: {exc}") from exc

        return self._normalize_history_frame(frame, ticker)

    def _normalize_history_frame(self, frame: pd.DataFrame, ticker: str) -> pd.DataFrame:
        """Normalize yfinance history output to the expected OHLCV schema."""

        if frame.empty:
            return frame

        normalized = frame.copy()

        if isinstance(normalized.columns, pd.MultiIndex):
            normalized.columns = [
                " ".join(str(level).strip() for level in levels if str(level).strip()).lower()
                for levels in normalized.columns
            ]
        else:
            normalized.columns = [str(col).strip().lower() for col in normalized.columns]

        normalized.index = pd.to_datetime(normalized.index)
        normalized.sort_index(inplace=True)

        missing = {column for column in EXPECTED_COLUMNS if column not in normalized.columns}
        if "adj close" in missing and "close" in normalized.columns:
            normalized["adj close"] = normalized["close"]
            missing.remove("adj close")

        if missing:
            ordered_missing = sorted(missing)
            raise DataIngestionError(
                f"Data for {ticker} missing expected columns after normalization: {ordered_missing}"
            )

        ordered = [column for column in EXPECTED_COLUMNS if column in normalized.columns]
        extras = [column for column in normalized.columns if column not in ordered]
        return normalized.loc[:, ordered + extras]

    def _load_from_cache(self, path: Path) -> pd.DataFrame:
        """Load parquet cache from disk with integrity checks."""

        try:
            frame = pd.read_parquet(path)
        except Exception as exc:  # pragma: no cover - corrupted cache fallback
            raise DataIngestionError(f"Cache at {path} is unreadable: {exc}") from exc

        if frame.empty:
            raise DataIngestionError(f"Cache at {path} is empty; delete and retry")
        return frame

    def _cache_path(
        self, ticker: str, start: datetime, end: datetime, interval: str
    ) -> Path:
        """Derive a unique cache filename for the request parameters."""

        safe_ticker = ticker.upper().replace("/", "-")
        key = f"{safe_ticker}_{start:%Y%m%d}_{end:%Y%m%d}_{interval}.parquet"
        return self._data_dir / key

    def _normalize_window(
        self, *, start: datetime, end: datetime, interval: str
    ) -> NormalizedWindow:
        """Clamp intraday ranges to API retention limits.

        Returns the adjusted start/end pair, a flag indicating whether the
        original request exceeded the supported window, and the retention cutoff
        when Yahoo Finance cannot serve the requested intraday history. All
        arithmetic stays O(1) because we operate on scalar datetimes.
        """

        lookback = self._intraday_lookback.get(interval)
        if lookback is None:
            return NormalizedWindow(start=start, end=end, truncated=False)

        truncated = False
        now_utc = datetime.utcnow()
        capped_end = min(end, now_utc)
        if capped_end != end:
            truncated = True

        retention_cutoff = now_utc - lookback
        if capped_end <= retention_cutoff:
            return NormalizedWindow(
                start=retention_cutoff,
                end=retention_cutoff,
                truncated=True,
                retention_cutoff=retention_cutoff,
            )

        normalized_start = start
        if start < retention_cutoff:
            normalized_start = retention_cutoff
            truncated = True

        if normalized_start >= capped_end:
            normalized_start = max(capped_end - timedelta(seconds=1), retention_cutoff)
            truncated = True

        return NormalizedWindow(start=normalized_start, end=capped_end, truncated=truncated)

    def _validate_frame(self, frame: pd.DataFrame, ticker: str) -> None:
        """Verify basic shape and presence of expected columns."""

        expected = set(EXPECTED_COLUMNS)
        missing = expected.difference(frame.columns)
        if missing:
            raise DataIngestionError(
                f"Data for {ticker} missing columns: {sorted(missing)}"
            )
        if len(frame) < self._config.min_rows:
            raise DataIngestionError(
                f"Data for {ticker} has fewer than {self._config.min_rows} rows"
            )


def default_ingestor(data_dir: Optional[Path] = None) -> MarketDataIngestor:
    """Convenience factory using the repo's `data/` directory."""

    base_dir = data_dir or Path.cwd() / "data"
    config = IngestionConfig(data_dir=base_dir)
    return MarketDataIngestor(config=config)


__all__ = [
    "DataIngestionError",
    "IngestionConfig",
    "MarketDataIngestor",
    "default_ingestor",
]
