from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest

from tradesentinel.data_ingestion import (
    DataIngestionError,
    IngestionConfig,
    MarketDataIngestor,
)


# Fixture is a parquet snapshot sourced from stooq's public daily feed because Yahoo Finance rate-limits
# automated downloads; the schema matches yfinance output for cache validation.
FIXTURE = Path(__file__).resolve().parent / "fixtures" / "AAPL_20200101_20200301_1d.parquet"


def _fixture_frame() -> pd.DataFrame:
    """Load deterministic OHLCV sample captured from historical quotes."""

    if not FIXTURE.exists():
        raise AssertionError(f"Fixture missing: {FIXTURE}")
    frame = pd.read_parquet(FIXTURE)
    frame.index = pd.to_datetime(frame.index)
    return frame


def test_fetch_price_history_caches_download(tmp_path) -> None:
    config = IngestionConfig(data_dir=tmp_path, min_rows=5)
    ingestor = MarketDataIngestor(config=config)
    frame = _fixture_frame()
    start = frame.index[0].to_pydatetime()
    end = frame.index[-1].to_pydatetime() + timedelta(days=1)

    with patch("tradesentinel.data_ingestion.yf.Ticker") as ticker_cls:
        ticker_instance = ticker_cls.return_value
        ticker_instance.history.return_value = frame
        downloaded = ingestor.fetch_price_history(
            ticker="AAPL", start=start, end=end, interval="1d"
        )
    assert not downloaded.empty
    assert ticker_instance.history.call_count == 1

    cached_files = list(tmp_path.glob("*.parquet"))
    assert len(cached_files) == 1

    with patch("tradesentinel.data_ingestion.yf.Ticker") as ticker_cls:
        ticker_instance = ticker_cls.return_value
        cached = ingestor.fetch_price_history(
            ticker="AAPL", start=start, end=end, interval="1d"
        )
    assert ticker_instance.history.call_count == 0
    pd.testing.assert_frame_equal(downloaded, cached, check_freq=False)


def test_fetch_price_history_validates_columns(tmp_path) -> None:
    config = IngestionConfig(data_dir=tmp_path, min_rows=5)
    ingestor = MarketDataIngestor(config=config)

    bad_frame = _fixture_frame().drop(columns=["volume"])
    start = bad_frame.index[0].to_pydatetime()
    end = bad_frame.index[-1].to_pydatetime() + timedelta(days=1)

    with patch("tradesentinel.data_ingestion.yf.Ticker") as ticker_cls:
        ticker_cls.return_value.history.return_value = bad_frame
        with pytest.raises(DataIngestionError):
            ingestor.fetch_price_history("AAPL", start=start, end=end)


def test_fetch_price_history_fills_adj_close_from_close(tmp_path) -> None:
    config = IngestionConfig(data_dir=tmp_path, min_rows=5)
    ingestor = MarketDataIngestor(config=config)

    frame = _fixture_frame().drop(columns=["adj close"])
    start = frame.index[0].to_pydatetime()
    end = frame.index[-1].to_pydatetime() + timedelta(days=1)

    with patch("tradesentinel.data_ingestion.yf.Ticker") as ticker_cls:
        ticker_instance = ticker_cls.return_value
        ticker_instance.history.return_value = frame
        normalized = ingestor.fetch_price_history(
            ticker="AAPL",
            start=start,
            end=end,
            interval="1d",
        )

    assert "adj close" in normalized.columns
    pd.testing.assert_series_equal(
        normalized["adj close"], normalized["close"], check_names=False
    )


def test_force_refresh_triggers_download_even_with_cache(tmp_path) -> None:
    config = IngestionConfig(data_dir=tmp_path, min_rows=5)
    ingestor = MarketDataIngestor(config=config)
    frame = _fixture_frame()
    start = frame.index[0].to_pydatetime()
    end = frame.index[-1].to_pydatetime() + timedelta(days=1)
    # Accessing the internal cache path keeps the test aligned with filename logic.
    cache_path = ingestor._cache_path("AAPL", start, end, "1d")
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(cache_path)

    with patch("tradesentinel.data_ingestion.yf.Ticker") as ticker_cls:
        ticker_instance = ticker_cls.return_value
        ticker_instance.history.return_value = frame
        ingestor.fetch_price_history(
            ticker="AAPL",
            start=start,
            end=end,
            interval="1d",
            force_refresh=True,
        )
    assert ticker_instance.history.call_count == 1


def test_intraday_requests_are_clamped_to_supported_window(tmp_path) -> None:
    config = IngestionConfig(data_dir=tmp_path, min_rows=5)
    ingestor = MarketDataIngestor(config=config)
    frame = _fixture_frame()
    end = datetime.utcnow() + timedelta(days=1)
    start = datetime(2018, 1, 1)

    with patch("tradesentinel.data_ingestion.yf.Ticker") as ticker_cls:
        ticker_instance = ticker_cls.return_value
        ticker_instance.history.return_value = frame
        ingestor.fetch_price_history(
            ticker="AAPL",
            start=start,
            end=end,
            interval="1h",
        )

    called_kwargs = ticker_instance.history.call_args.kwargs
    normalized_start = called_kwargs["start"]
    normalized_end = called_kwargs["end"]
    lookback = timedelta(days=60)
    now_approx = datetime.utcnow()

    assert normalized_end <= now_approx
    assert abs((normalized_end - now_approx).total_seconds()) < 5
    expected_start = normalized_end - lookback
    assert abs((normalized_start - expected_start).total_seconds()) < 5


def test_intraday_request_before_retention_raises(tmp_path) -> None:
    config = IngestionConfig(data_dir=tmp_path, min_rows=5)
    ingestor = MarketDataIngestor(config=config)

    start = datetime(2020, 1, 1)
    end = datetime(2020, 1, 31)

    with pytest.raises(DataIngestionError) as excinfo:
        ingestor.fetch_price_history(
            ticker="AAPL",
            start=start,
            end=end,
            interval="1h",
        )

    message = str(excinfo.value)
    assert "retention" in message
    assert "daily" in message
