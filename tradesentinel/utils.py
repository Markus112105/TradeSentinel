"""Shared helper utilities for the trading dashboard package.

This module holds pure helper functions that do not warrant standalone modules
but are reused across ingestion, analytics, or the dashboard. Keep logic small
and dependency-free to preserve reusability and simplify unit testing.
"""
from __future__ import annotations

from typing import Iterable


def chunk_iterable(iterable: Iterable, size: int):
    """Yield successive chunks of `size` from `iterable`.

    Complexity: O(n) traversal with constant space aside from the chunk buffer.
    Useful for batching API requests or writing data in segments.
    """

    if size <= 0:
        raise ValueError("Chunk size must be positive")

    chunk = []
    for item in iterable:
        chunk.append(item)
        if len(chunk) == size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


__all__ = ["chunk_iterable"]
