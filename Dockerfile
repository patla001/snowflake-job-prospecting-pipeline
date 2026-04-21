# Extend official Airflow 2.11 (matches docker-compose) — add Snowflake provider only.
FROM apache/airflow:2.11.2-python3.11

COPY requirements-airflow-docker.txt /requirements-airflow-docker.txt
RUN pip install --no-cache-dir -r /requirements-airflow-docker.txt
