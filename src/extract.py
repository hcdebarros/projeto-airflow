import shutil
from datetime import datetime
from .config import RAW_DIR, BRONZE_DIR

def extract_to_bronze(**context):
    src_file = RAW_DIR / "US_Accidents.csv"

    if not src_file.exists():
        raise FileNotFoundError(f"Arquivo RAW não encontrado: {src_file}")

    # Usa data atual
    dt = datetime.utcnow()
    bronze_file = BRONZE_DIR / f"US_Accidents_{dt:%Y%m%d}.csv"

    shutil.copy(src_file, bronze_file)

    print(f"[BRONZE] Copiado {src_file} → {bronze_file}")
    return str(bronze_file)
