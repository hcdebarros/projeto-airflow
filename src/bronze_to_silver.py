import duckdb
from .config import BRONZE_DIR, SILVER_DIR, DUCKDB_PATH

def bronze_to_silver(**context):

    bronze_files = list(BRONZE_DIR.glob("US_Accidents_*.csv"))
    if not bronze_files:
        raise FileNotFoundError("Nenhum arquivo CSV encontrado na Bronze.")

    bronze_file = bronze_files[-1]
    silver_file = SILVER_DIR / "US_Accidents_Silver.parquet"

    con = duckdb.connect(str(DUCKDB_PATH))

    # bronze_raw
    con.execute(f"""
        CREATE OR REPLACE TABLE bronze_raw AS
        SELECT *
        FROM read_csv_auto('{bronze_file}', IGNORE_ERRORS=TRUE);
    """)

    # silver_clean — SEM try_strptime
    con.execute("""
        CREATE OR REPLACE TABLE silver_clean AS
        SELECT
            ID,
            CAST(Severity AS INTEGER) AS Severity,

            Start_Time,   -- DuckDB já converte
            End_Time,

            City,
            State,
            Country,

            CAST("Distance(mi)" AS DOUBLE) AS Distance,
            "Weather_Condition" AS Weather_Condition,
            CAST("Temperature(F)" AS DOUBLE) AS Temperature,
            CAST("Visibility(mi)" AS DOUBLE) AS Visibility

        FROM bronze_raw
        WHERE Start_Time IS NOT NULL
          AND State IS NOT NULL
          AND Country = 'US';
    """)

    # salva parquet
    con.execute(f"""
        COPY silver_clean TO '{silver_file}'
        (FORMAT PARQUET, COMPRESSION 'zstd');
    """)

    print(f"[SILVER] Dataset limpo gerado: {silver_file}")
    con.close()
    return str(silver_file)
