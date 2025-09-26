# PROJECT_STRUCTURE.md — Trading Dashboard

## Repository Layout

TradeSentinel/
│
├── tradesentinel/ # Source code package
│ ├── __init__.py
│ ├── __main__.py # Enables `python -m tradesentinel`
│ ├── backtester.py # Backtesting engine and portfolio logic
│ ├── dashboard.py # Streamlit visualization layer
│ ├── data_ingestion.py # Fetch and cache market data from Yahoo Finance
│ ├── indicators.py # Technical indicators (EMA, RSI, volatility, etc.)
│ └── utils.py # Shared helper functions
│
├── tests/ # Unit tests for core modules and dashboard helpers
│ ├── conftest.py
│ ├── fixtures/
│ ├── test_backtester.py
│ ├── test_dashboard.py
│ ├── test_data_ingestion.py
│ ├── test_indicators.py
│ └── test_utils.py
│
├── docs/ # Project documentation and design notes
│ ├── AGENTS.md
│ ├── DESIGN_NOTES.md
│ └── PROJECT_STRUCTURE.md
│
├── notebooks/ # Exploratory analysis notebooks (not part of runtime path)
│
├── data/ # Parquet caches created by the ingestion module (gitignored)
│
├── requirements.txt # Python dependencies
└── README.md # Overview and instructions

---

## Design Philosophy

- **Modularity:** Each file has a clear, single responsibility.
- **Testability:** All critical logic in `tradesentinel/` must be covered by `tests/`.
- **Reproducibility:** Anyone should be able to run the system by installing from `requirements.txt` and using the cached parquet artifacts.
- **Clarity:** Notebooks are exploratory only; production code lives in `tradesentinel/`.

---

## Next Steps

1. Initialize repo with this structure.
2. Write stubs for all modules with docstrings describing intended functionality.
3. Create a minimal working pipeline:
   - Fetch data with `data_ingestion.py`
   - Compute simple EMA crossover in `indicators.py`
   - Run one backtest with `backtester.py`
   - Display results in `dashboard.py`
4. Expand iteratively with additional features, metrics, and UI improvements.
