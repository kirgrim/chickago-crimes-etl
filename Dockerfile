FROM apache/airflow:2.9.1-python3.12

COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt

COPY --chown=airflow:root chickago_crimes_etl/dags/ /opt/airflow/dags
COPY --chown=airflow:root chickago_crimes_etl/loaders/ /chickago_crimes_etl/loaders
COPY --chown=airflow:root chickago_crimes_etl/processors/ /chickago_crimes_etl/processors
COPY --chown=airflow:root chickago_crimes_etl/utils/ /chickago_crimes_etl/utils

ENV PYTHONPATH "/:${PYTHONPATH}"