# Astronomer / Astro — Airflow 3.x (Astro Runtime 3.2). Registry per:
# https://www.astronomer.io/docs/runtime/runtime-image-architecture.md
FROM astrocrpublic.azurecr.io/runtime:3.2-2

COPY requirements.txt /requirements.txt
USER root
RUN pip install --no-cache-dir -r /requirements.txt
USER astro
