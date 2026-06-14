"""Root conftest: ensure src/ is at the front of sys.path.

The repo still contains the legacy flat-layout paper_sorts/ directory
(to be removed in T036).  Without this hook, Python picks up the old
package before the modernized src/paper_sorts/ one.

This file is loaded by pytest before any test collection happens.
"""

from __future__ import annotations

import sys
from pathlib import Path

# Prepend src/ so the new src-layout package is found before the legacy root package.
_src = str(Path(__file__).parent / "src")
if _src not in sys.path:
    sys.path.insert(0, _src)
