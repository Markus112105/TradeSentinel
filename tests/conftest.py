from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
PKG_PATH = PROJECT_ROOT / "tradesentinel"
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
if str(PKG_PATH) not in sys.path:
    sys.path.insert(0, str(PKG_PATH))
