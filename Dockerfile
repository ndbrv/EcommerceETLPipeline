FROM apache/airflow:2.7.3-python3.11

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow
ENV PIP_BREAK_SYSTEM_PACKAGES=1

RUN pip install --no-cache-dir \
    snowflake-connector-python==3.6.0 \
    pandas==2.1.0 \
    pyarrow==13.0.0 \
    faker==19.6.0 \
    python-dotenv==1.0.0 \
    requests==2.31.0
