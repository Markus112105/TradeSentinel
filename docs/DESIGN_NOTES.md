# DESIGN_NOTES.md — Trading Dashboard

This document captures architectural decisions that do not fit inline in the
modules. Expand with detailed rationales as features evolve.

- Data ingestion caches parquet snapshots per ticker/time-window pair to keep
  repeated backtests deterministic and IO efficient.
- Indicator utilities accept pandas Series to avoid bespoke data structures and
  ensure all heavy math stays vectorized for O(n) complexity.
- The backtester isolates risky state transitions (positions, turnover, equity)
  in one place, simplifying audits and unit testing.
- The Streamlit dashboard is intentionally thin: it binds user inputs to the
  analytical core so that the same logic remains reusable in non-UI contexts.
- Offline integration tests rely on cached parquet fixtures generated from
  stooq's public data feed because Yahoo Finance aggressively rate-limits
  unattended yfinance calls; the schema mirrors yfinance output so caching
  semantics remain identical.

Add future decisions here, including rejected alternatives and trade-off
analysis.
