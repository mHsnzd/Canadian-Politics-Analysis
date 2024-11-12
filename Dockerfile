FROM apache/airflow:2.7.1-python3.9

USER root

COPY requirements.txt /opt/airflow/

RUN apt-get update && apt-get install -y gcc python3-dev

RUN apt-get update && \
    apt-get install -y libpq-dev gcc && \
    rm -rf /var/lib/apt/lists/*

USER airflow

RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt
