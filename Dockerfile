FROM apache/airflow:2.9.1-python3.12

COPY requirements.txt /

RUN pip install --no-cache-dir -r /requirements.txt

# Microsoft ODBC installation
USER root
COPY microsoft.asc /etc/apt/trusted.gpg.d/microsoft.asc
COPY prod.list /etc/apt/sources.list.d/mssql-release.list
RUN apt-get update
RUN sudo ACCEPT_EULA=Y apt-get install -y msodbcsql18
USER airflow

COPY --chown=airflow:root chickago_crimes_etl/dags/ /opt/airflow/dags
COPY --chown=airflow:root chickago_crimes_etl/loaders/ /chickago_crimes_etl/loaders
COPY --chown=airflow:root chickago_crimes_etl/processors/ /chickago_crimes_etl/processors
COPY --chown=airflow:root chickago_crimes_etl/utils/ /chickago_crimes_etl/utils

ENV PYTHONPATH="/:/chickago_crimes_etl:${PYTHONPATH}"