from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime

SNOWFLAKE_CONN_ID = Variable.get("snowflake_conn_id", default_var="snowflake_conn")
DATABASE = Variable.get("snowflake_database", default_var="USER_DB_HORNET")

def get_cursor():
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    conn = hook.get_conn()
    return conn, conn.cursor()

@task
def build_check_publish():
    conn, cur = get_cursor()
    try:
        cur.execute("BEGIN;")
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {DATABASE}.ANALYTICS;")

        # 1) Build into a TEMP table (session-scoped)
        cur.execute(f"""
            CREATE TEMPORARY TABLE {DATABASE}.ANALYTICS.SESSION_SUMMARY_TMP AS
            WITH user_dedup AS (
                SELECT SESSIONID, MIN(USERID) AS USERID, MIN(CHANNEL) AS CHANNEL
                FROM {DATABASE}.RAW.USER_SESSION_CHANNEL
                GROUP BY SESSIONID
            ),
            ts_dedup AS (
                SELECT SESSIONID, MAX(TS) AS TS
                FROM {DATABASE}.RAW.SESSION_TIMESTAMP
                GROUP BY SESSIONID
            ),
            joined AS (
                SELECT u.SESSIONID, u.USERID, u.CHANNEL, t.TS
                FROM user_dedup u
                INNER JOIN ts_dedup t ON u.SESSIONID = t.SESSIONID
            )
            SELECT *
            FROM joined
            QUALIFY ROW_NUMBER() OVER (PARTITION BY SESSIONID ORDER BY TS DESC) = 1;
        """)

        # 2) Log counts
        cur.execute(f"SELECT COUNT(*) FROM {DATABASE}.ANALYTICS.SESSION_SUMMARY_TMP;")
        total = cur.fetchone()[0]
        cur.execute(f"SELECT COUNT(DISTINCT SESSIONID) FROM {DATABASE}.ANALYTICS.SESSION_SUMMARY_TMP;")
        distinct_sessions = cur.fetchone()[0]
        print(f"SESSION_SUMMARY_TMP rows={total}, distinct_sessionid={distinct_sessions}")

        # 3) Duplicate check (fail before publish)
        if distinct_sessions < total:
            cur.execute("ROLLBACK;")
            raise Exception("Duplicate SESSIONID detected in temp result. Aborting publish.")

        # 4) Publish
        cur.execute(f"""
            CREATE OR REPLACE TABLE {DATABASE}.ANALYTICS.SESSION_SUMMARY AS
            SELECT * FROM {DATABASE}.ANALYTICS.SESSION_SUMMARY_TMP;
        """)

        cur.execute("COMMIT;")
        print(f"Published ANALYTICS.SESSION_SUMMARY with {total} rows.")
    except Exception:
        try:
            cur.execute("ROLLBACK;")
        finally:
            pass
        raise
    finally:
        cur.close(); conn.close()

with DAG(
    dag_id="session_summary_elt",
    start_date=datetime(2025, 10, 1),
    schedule=None,  # triggered from your ETL DAG
    catchup=False,
    tags=["ELT", "analytics", "snowflake", "wau_etl"],
) as dag:
    build_check_publish()