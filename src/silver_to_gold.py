import duckdb
from .config import SILVER_DIR, GOLD_DIR, DUCKDB_PATH

def silver_to_gold(**context):
    con = duckdb.connect(str(DUCKDB_PATH))

    silver_file = SILVER_DIR / "US_Accidents_Silver.parquet"
    gold_file   = GOLD_DIR / "accidents_by_state.parquet"

    # recria a tabela silver_clean a partir do parquet
    con.execute(f"""
        CREATE OR REPLACE TABLE silver_clean AS
        SELECT *
        FROM read_parquet('{silver_file}');
    """)

    # cria tabela gold com KPIs
    con.execute("""
        CREATE OR REPLACE TABLE gold_accidents_by_state AS
        SELECT 
            State AS state,
            COUNT(*) AS total_accidents,
            AVG(Severity) AS avg_severity
        FROM silver_clean
        GROUP BY State
        ORDER BY total_accidents DESC;
    """)

    # salva gold em Parquet
    con.execute(f"""
        COPY gold_accidents_by_state TO '{gold_file}'
        (FORMAT PARQUET, COMPRESSION 'zstd');
    """)

    print(f"[GOLD] KPIs criados em: {gold_file}")

    con.close()
    return str(gold_file)
