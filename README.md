# ELT Pipeline — US Accidents (Airflow + DuckDB + Docker)

Pipeline ELT completo utilizando **Apache Airflow**, **DuckDB** e **Docker**, com ingestão, transformação e geração de KPIs a partir do dataset público **US Accidents** (Kaggle).  
O projeto segue arquitetura *medallion* (Bronze → Silver → Gold).

## Tecnologias utilizadas
- Docker + Docker Compose
- Apache Airflow 3.1
- DuckDB
- Python 3.12
- Arquitetura Bronze/Silver/Gold

## Arquitetura do Projeto
```
projeto_elt/
│
├── dags/
│   └── elt_us_accidents.py
│
├── src/
│   ├── extract.py
│   ├── bronze_to_silver.py
│   ├── silver_to_gold.py
│   └── config.py
│
├── data/
│   ├── raw/
│   ├── bronze/
│   ├── silver/
│   ├── gold/
│   └── warehouse.duckdb
│
├── docker-compose.yaml
└── README.md
```

## Como funciona o pipeline
### 1️⃣ Extract → Bronze
- Copia o dataset RAW
- Gera arquivo versionado
- Salva em `/data/bronze`

### 2️⃣ Bronze → Silver
- Lê CSV com DuckDB
- Converte tipos e limpa dados
- Salva Parquet em `/data/silver`

### 3️⃣ Silver → Gold
Gera KPIs:
- Acidentes por estado
- Severidade média
- Salva Parquet em `/data/gold`

## Como rodar o projeto
### 1. Clonar o repositório
```
git clone https://github.com/SEU_USUARIO/projeto_elt.git
cd projeto_elt
```

### 2. Subir o Airflow
```
docker compose up -d
```

### 3. Acessar o Airflow
```
http://localhost:8080
User: airflow
Pass: airflow
```

### 4. Colocar o dataset
Baixe do Kaggle e coloque em:
```
link do dataset: https://www.kaggle.com/datasets/sobhanmoosavi/us-accidents?select=US_Accidents_March23.csv
```
```
data/raw/US_Accidents.csv
```

### 5. Executar a DAG
Airflow → `elt_us_accidents` → Run.

## Exemplo de resultado Gold
| state | total_accidents | avg_severity |
|-------|-----------------|---------------|
| CA    | 1,741,433       | 2.16 |
| FL    | 880,192         | 2.14 |
| TX    | 582,837         | 2.22 |

## .gitignore incluído
- data/
- logs/
- .env
- __pycache__
- venv/

## Autor
**Helder Barros**  

