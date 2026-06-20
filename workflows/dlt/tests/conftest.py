import sys
from pathlib import Path

# src/ layout: runner lives under src/, project packages under projects/.
_root = Path(__file__).parent.parent
sys.path.insert(0, str(_root / "src"))
sys.path.insert(0, str(_root / "projects"))
