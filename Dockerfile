ARG AIRFLOW_VERSION=2.10.5
FROM apache/airflow:${AIRFLOW_VERSION}-python3.12

ARG AIRFLOW_VERSION=2.10.5
ARG PYTHON_VERSION=3.12

USER airflow

COPY requirements.txt /tmp/requirements.txt

RUN python -m pip install --no-cache-dir \
    "apache-airflow==${AIRFLOW_VERSION}" \
    --requirement /tmp/requirements.txt \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
