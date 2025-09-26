"""Package entry point for `python -m tradesentinel`."""

from tradesentinel.dashboard import main as run_dashboard


def main() -> None:
    """Delegate to the dashboard module for CLI execution."""

    run_dashboard()


if __name__ == "__main__":  # pragma: no cover - module execution entry point
    main()
