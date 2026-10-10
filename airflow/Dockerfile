# Learning deployment. Image/runtime checks remain pending without Docker.
FROM apache/airflow:3.3.2-python3.12
USER root
RUN mkdir -p /opt/app-venv /opt/dbt-venv && chown -R airflow:0 /opt/app-venv /opt/dbt-venv
USER airflow
COPY --chown=airflow:0 requirements/ /tmp/mobilityops-requirements/
RUN python -m venv /opt/app-venv && /opt/app-venv/bin/pip install --no-cache-dir -r /tmp/mobilityops-requirements/local.lock.txt
RUN python -m venv /opt/dbt-venv && /opt/dbt-venv/bin/pip install --no-cache-dir -r /tmp/mobilityops-requirements/dbt-local.lock.txt
