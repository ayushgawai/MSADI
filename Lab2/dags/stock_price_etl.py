from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
import random

# Connection and database config from Airflow Variables
SNOWFLAKE_CONN_ID = Variable.get("snowflake_conn_id", default_var="snowflake_conn")
DATABASE = Variable.get("snowflake_database", default_var="USER_DB_HORNET")

# Stock symbols and days to generate
SYMBOLS = ["AAPL", "MSFT", "GOOGL", "AMZN"]
DAYS_BACK = 180

# Base prices for realistic data
BASE_PRICES = {
    "AAPL": 175.0,
    "MSFT": 380.0,
    "GOOGL": 140.0,
    "AMZN": 180.0,
}

def get_snowflake_conn():
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    return hook.get_conn()

@task
def extract_data():
    """
    Generate realistic OHLCV stock data for multiple symbols.
    Uses random walk with drift to simulate real price movements.
    """
    random.seed(42)  # For reproducibility
    
    out = []
    end_date = datetime.now()
    
    for sym in SYMBOLS:
        price = BASE_PRICES[sym]
        
        for i in range(DAYS_BACK, 0, -1):
            date = end_date - timedelta(days=i)
            
            # Skip weekends
            if date.weekday() >= 5:
                continue
            
            # Random daily change (-3% to +3%)
            daily_return = random.gauss(0.0005, 0.015)
            price = price * (1 + daily_return)
            
            # Generate OHLCV
            open_price = price * (1 + random.uniform(-0.005, 0.005))
            high_price = max(open_price, price) * (1 + random.uniform(0, 0.01))
            low_price = min(open_price, price) * (1 - random.uniform(0, 0.01))
            close_price = price
            volume = int(random.uniform(10_000_000, 100_000_000))
            
            out.append({
                "SYMBOL": sym,
                "Date": date.strftime("%Y-%m-%d"),
                "Open": round(open_price, 4),
                "High": round(high_price, 4),
                "Low": round(low_price, 4),
                "Close": round(close_price, 4),
                "Volume": volume,
            })
    
    print(f"Generated {len(out)} records for {len(SYMBOLS)} symbols")
    return out

@task
def transform_data(raw_rows):
    """Type/validate rows -> (symbol, date_str, open, close, high, low, volume)."""
    records = []
    for r in raw_rows:
        try:
            records.append((
                r["SYMBOL"],
                r["Date"],
                float(r["Open"]),
                float(r["Close"]),
                float(r["High"]),
                float(r["Low"]),
                int(r["Volume"]),
            ))
        except Exception as e:
            print(f"Skip row: {e}")
    
    if not records:
        raise ValueError("No records after transform")
    return records

@task
def load_raw(records):
    """Full refresh into RAW.STOCK_PRICE (transaction)."""
    tgt = f"{DATABASE}.RAW.STOCK_PRICE"
    conn = get_snowflake_conn()
    cur = conn.cursor()
    try:
        cur.execute("BEGIN;")
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {DATABASE}.RAW;")
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {tgt} (
              SYMBOL VARCHAR(20) NOT NULL,
              DATE   DATE NOT NULL,
              OPEN   DECIMAL(12,4) NOT NULL,
              CLOSE  DECIMAL(12,4) NOT NULL,
              HIGH   DECIMAL(12,4) NOT NULL,
              LOW    DECIMAL(12,4) NOT NULL,
              VOLUME BIGINT NOT NULL,
              PRIMARY KEY (SYMBOL, DATE)
            );
        """)
        cur.execute(f"DELETE FROM {tgt};")
        cur.executemany(
            f"INSERT INTO {tgt} (SYMBOL, DATE, OPEN, CLOSE, HIGH, LOW, VOLUME) VALUES (%s,%s,%s,%s,%s,%s,%s)",
            records
        )
        cur.execute("COMMIT;")
        print(f"Loaded {len(records)} rows into {tgt}")
    except Exception:
        try:
            cur.execute("ROLLBACK;")
        except:
            pass
        raise
    finally:
        cur.close()
        conn.close()

with DAG(
    dag_id="stock_price_etl",
    start_date=datetime(2025, 10, 1),
    schedule="15 3 * * *",
    catchup=False,
    tags=["ETL", "stock_data"],
    description="ETL pipeline to load stock price data into Snowflake RAW layer",
) as dag:
    raw = extract_data()
    transformed = transform_data(raw)
    load_ok = load_raw(transformed)

    # Trigger dbt DAG for ELT transformations (Lab 2)
    trigger_dbt = TriggerDagRunOperator(
        task_id="trigger_dbt_stock_analytics",
        trigger_dag_id="dbt_stock_analytics",
        wait_for_completion=False,
    )

    # Trigger ML forecast DAG (Lab 1)
    trigger_ml_forecast = TriggerDagRunOperator(
        task_id="trigger_sf_ml_forcast_dag",
        trigger_dag_id="sf_ml_forcast_dag",
        wait_for_completion=False,
    )

    load_ok >> [trigger_dbt, trigger_ml_forecast]

