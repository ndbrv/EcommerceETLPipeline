FROM apache/airflow:2.7.3-python3.11

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Install Python packages (removed PIP_BREAK_SYSTEM_PACKAGES)
RUN pip install --no-cache-dir --user \
    snowflake-connector-python==3.7.0 \
    pandas==2.1.2 \
    pyarrow==14.0.1 \
    faker==19.6.0 \
    python-dotenv==1.0.0 \
    requests==2.31.0 \
    apache-airflow-providers-snowflake==5.1.0 \
    dbt-core==1.7.0 \
    dbt-snowflake==1.7.0 \
    astronomer-cosmos==1.2.0
