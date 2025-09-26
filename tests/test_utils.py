from __future__ import annotations

import pytest

from tradesentinel import utils


def test_chunk_iterable_splits_evenly() -> None:
    chunks = list(utils.chunk_iterable(range(6), size=2))
    assert chunks == [[0, 1], [2, 3], [4, 5]]


def test_chunk_iterable_rejects_non_positive_size() -> None:
    with pytest.raises(ValueError):
        list(utils.chunk_iterable([1, 2, 3], size=0))
