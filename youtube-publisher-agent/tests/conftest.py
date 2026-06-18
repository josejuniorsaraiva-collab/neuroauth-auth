import sys
from pathlib import Path

# Garante que o pacote 'publisher' seja importável ao rodar pytest da raiz.
ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
