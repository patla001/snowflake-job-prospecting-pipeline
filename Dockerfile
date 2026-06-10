# Astronomer / Astro — Airflow 3.x (Astro Runtime 3.2). Registry per:
# https://www.astronomer.io/docs/runtime/runtime-image-architecture.md
FROM astrocrpublic.azurecr.io/runtime:3.2-2

COPY requirements.txt /requirements.txt
USER root
RUN pip install --no-cache-dir -r /requirements.txt

# Astro Runtime's ONBUILD copies dags/, plugins/, include/ only — get dbt/
# into the image explicitly so pems_dbt_build can find its project.
COPY dbt /usr/local/airflow/dbt

# dbt-snowflake lives in its own venv to avoid clashing with Airflow 3's
# task-SDK deps. Read by pems_dbt_build DAG at /opt/dbt-venv.
RUN python -m venv /opt/dbt-venv && \
    /opt/dbt-venv/bin/pip install --no-cache-dir --upgrade pip && \
    /opt/dbt-venv/bin/pip install --no-cache-dir 'dbt-snowflake>=1.8,<2' && \
    chown -R astro:0 /opt/dbt-venv
USER astro
