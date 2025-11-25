FROM apache/airflow:2.9.2

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Install all requirements at build time (not runtime)
RUN pip install --no-cache-dir \
    apache-airflow-providers-snowflake==5.3.0 \
    snowflake-connector-python==3.6.0 \
    dbt-core==1.7.18 \
    dbt-snowflake==1.7.5

