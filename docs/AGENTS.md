# AGENTS.md — Trading Dashboard

## Purpose
This document defines the collaboration rules, coding style, and review standards for the Trading Dashboard project. The goal is to ensure that all generated or contributed code is **clear, defensible, and production-oriented**.

---

## Agent Principles

1. **Pushback Against Bad Ideas**
   - If a prompt or request proposes an inefficient, insecure, or unnecessarily complex approach, the agent must **push back**.
   - Provide a **clear explanation** of why the idea is suboptimal, including potential performance, maintainability, or clarity issues.
   - Always propose a **better alternative** with justification.

   **Example:**
   - Bad Prompt: “Use a global variable to store open positions.”
   - Pushback: Global variables create hidden dependencies and make testing difficult. A better approach is to encapsulate open positions in a `Portfolio` class, which makes state explicit and testable.

2. **Explain Trade-offs**
   - All design choices should include reasoning about complexity (time/space), scalability, and readability.
   - Example: “We use a deque here instead of a list because append/pop operations are O(1), which matters for rolling-window calculations.”

---

## Commenting Standards

1. **No Emojis.**
2. **Descriptive, instructional comments only.**
   - Comments must explain the *why*, not just the *what*.
   - Anyone with a strong CS background should be able to understand the logic, even without deep finance or language-specific expertise.
3. **Outline data structures and algorithms used.**
   - Example: “We store trades in a dictionary keyed by ticker symbol. This gives O(1) lookup when checking if a position is already open.”
   - Example: “EMA calculation uses exponential smoothing with alpha = 2/(n+1), implemented via pandas `.ewm` for clarity and efficiency.”

---

## Coding Guidelines

1. **Data Structures**
   - Use appropriate abstractions:
     - `dict` / hash maps for O(1) lookups.
     - `deque` for sliding window operations.
     - `DataFrame` for vectorized time series analysis.
   - Avoid global state; encapsulate logic in classes or functions.

2. **Algorithm Choices**
   - Document time complexity for core loops or calculations.
   - Prefer vectorized operations (NumPy, Pandas) over manual loops where clarity is not sacrificed.
   - For backtesting logic: explain trade entry/exit rules in plain English in comments.

3. **Error Handling**
   - Anticipate bad data (e.g., missing price history, API downtime).
   - Fail gracefully with descriptive error messages.

4. **Reproducibility**
   - All scripts must be runnable from a clean environment using `requirements.txt`.
   - Random processes (e.g., simulations) must include fixed seeds for determinism when testing.

---

## Deliverables

- **Core Modules:**
  - `data_ingestion.py` — download and cache market data.
  - `indicators.py` — implement EMA, RSI, volatility calculations.
  - `backtester.py` — trading logic, portfolio tracking, performance metrics.
  - `dashboard.py` — user-facing visualization (likely Streamlit or Dash).

- **Documentation:**
  - Each module must include a docstring summarizing purpose and dependencies.
  - Inline comments must clarify algorithm/data structure usage.

---

## Review Checklist

Before merging any code, check:
- Are all data structures and algorithms explained in comments?
- Are complexity trade-offs described?
- Does the code avoid hidden state and global variables?
- Did the agent push back on any naive or questionable implementation choices?
- Is the approach reproducible and testable?

