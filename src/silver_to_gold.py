import duckdb
from .config import SILVER_DIR, GOLD_DIR, DUCKDB_PATH

def silver_to_gold(**context):
    con = duckdb.connect(str(DUCKDB_PATH))

    silver_file = SILVER_DIR / "US_Accidents_Silver.parquet"

    # KPIs a serem gerados (arquivos de saída)
    file_state       = GOLD_DIR / "gold_accidents_by_state.parquet"
    file_city        = GOLD_DIR / "gold_accidents_by_city.parquet"
    file_weather_dist = GOLD_DIR / "gold_weather_distribution.parquet"
    file_by_month    = GOLD_DIR / "gold_accidents_by_month.parquet"
    file_weather_sev = GOLD_DIR / "gold_severity_by_weather.parquet"

    # RECRIA A SILVER NA MEMÓRIA
    con.execute(f"""
        CREATE OR REPLACE TABLE silver_clean AS
        SELECT *
        FROM read_parquet('{silver_file}');
    """)

    # ======================================
    # GOLD 1 — Acidentes por estado
    # ======================================
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

    con.execute(f"""
        COPY gold_accidents_by_state TO '{file_state}'
        (FORMAT PARQUET, COMPRESSION 'zstd');
    """)

    # ======================================
    # GOLD 2 — Acidentes por cidade (Top 20)
    # ======================================
    con.execute("""
        CREATE OR REPLACE TABLE gold_accidents_by_city AS
        SELECT 
            City AS city,
            COUNT(*) AS total_accidents
        FROM silver_clean
        WHERE City IS NOT NULL AND City <> ''
        GROUP BY City
        ORDER BY total_accidents DESC
        LIMIT 20;
    """)

    con.execute(f"""
        COPY gold_accidents_by_city TO '{file_city}'
        (FORMAT PARQUET, COMPRESSION 'zstd');
    """)

    # ======================================
    # GOLD 3 — Distribuição de condições climáticas
    # ======================================
    con.execute("""
        CREATE OR REPLACE TABLE gold_weather_distribution AS
        SELECT
            Weather_Condition AS weather_condition,
            COUNT(*) AS occurrences
        FROM silver_clean
        WHERE Weather_Condition IS NOT NULL AND Weather_Condition <> ''
        GROUP BY Weather_Condition
        ORDER BY occurrences DESC;
    """)

    con.execute(f"""
        COPY gold_weather_distribution TO '{file_weather_dist}'
        (FORMAT PARQUET, COMPRESSION 'zstd');
    """)

    # ======================================
    # GOLD 4 — Acidentes por mês
    # ======================================
    con.execute("""
        CREATE OR REPLACE TABLE gold_accidents_by_month AS
        SELECT
            EXTRACT(month FROM Start_Time) AS month,
            COUNT(*) AS total_accidents
        FROM silver_clean
        GROUP BY month
        ORDER BY month;
    """)

    con.execute(f"""
        COPY gold_accidents_by_month TO '{file_by_month}'
        (FORMAT PARQUET, COMPRESSION 'zstd');
    """)

    # ======================================
    # GOLD 5 — Relação clima × severidade
    # ======================================
    con.execute("""
        CREATE OR REPLACE TABLE gold_severity_by_weather AS
        SELECT
            Weather_Condition AS weather_condition,
            COUNT(*) AS occurrences,
            AVG(Severity) AS avg_severity
        FROM silver_clean
        WHERE Weather_Condition IS NOT NULL AND Weather_Condition <> ''
        GROUP BY Weather_Condition
        ORDER BY avg_severity DESC;
    """)

    con.execute(f"""
        COPY gold_severity_by_weather TO '{file_weather_sev}'
        (FORMAT PARQUET, COMPRESSION 'zstd');
    """)

    print("[GOLD] Todos os 5 KPIs foram salvos em Parquet com sucesso!")

    con.close()

    return {
        "state": str(file_state),
        "city": str(file_city),
        "weather_dist": str(file_weather_dist),
        "month": str(file_by_month),
        "weather_severity": str(file_weather_sev),
    }
