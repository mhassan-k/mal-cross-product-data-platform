FROM apache/airflow:2.10.5-python3.10

COPY requirements-docker.txt /tmp/
RUN pip install --no-cache-dir -r /tmp/requirements-docker.txt

COPY --chown=airflow:root ./src /opt/airflow/src
COPY --chown=airflow:root ./dags /opt/airflow/dags
COPY --chown=airflow:root ./data /opt/airflow/data
COPY --chown=airflow:root ./sql /opt/airflow/sql
COPY --chown=airflow:root ./tests /opt/airflow/tests
COPY --chown=airflow:root ./entrypoint.sh /opt/airflow/entrypoint.sh

RUN mkdir -p /opt/airflow/shared /opt/airflow/data/output
