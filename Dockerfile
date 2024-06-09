FROM apache/airflow:2.9.1-python3.12

COPY requirements.txt /

RUN pip install --no-cache-dir -r /requirements.txt

# Microsoft ODBC installation
USER root
COPY microsoft.asc /etc/apt/trusted.gpg.d/microsoft.asc
COPY prod.list /etc/apt/sources.list.d/mssql-release.list
RUN apt-get update
RUN sudo ACCEPT_EULA=Y apt-get install -y msodbcsql17 unixodbc-dev
USER airflow

COPY --chown=airflow:root chickago_crimes_etl/dags/ /opt/airflow/dags
COPY --chown=airflow:root chickago_crimes_etl/loaders/ /chickago_crimes_etl/loaders
COPY --chown=airflow:root chickago_crimes_etl/processors/ /chickago_crimes_etl/processors
COPY --chown=airflow:root chickago_crimes_etl/utils/ /chickago_crimes_etl/utils

ENV PYTHONPATH="/:/chickago_crimes_etl:${PYTHONPATH}"
ENV TRAFFIC_CRASHES_PEOPLE_SOURCE_PATH="/chickago_crimes_etl/src/Traffic_Crashes_People.csv"
ENV TRAFFIC_CRASHES_CRASHES_SOURCE_PATH="/chickago_crimes_etl/src/Traffic_Crashes_Crashes.csv"
ENV TRAFFIC_CRASHES_VEHICLES_SOURCE_PATH="/chickago_crimes_etl/src/Traffic_Crashes_Vehicles.csv"
ENV DIM_FILES_DIR="/chickago_crimes_etl/dim/"
ENV DB_SERVER_URL="mssql-db:1433"
ENV FILE_PROCESSING_CHUNK_SIZE=1000000
