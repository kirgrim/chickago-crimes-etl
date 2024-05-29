import uuid

from airflow.decorators import dag, task

from chickago_crimes_etl.loaders.traffic_crashes_accident_time_loader import TrafficCrashesAccidentDateLoader
from chickago_crimes_etl.loaders.traffic_crashes_data_loader import TrafficCrashesDataLoader
from chickago_crimes_etl.loaders.traffic_crashes_locations_loader import TrafficCrashesLocationsLoader
from chickago_crimes_etl.loaders.traffic_crashes_police_notified_date_loader import \
    TrafficCrashesPoliceNotifiedDateLoader
from chickago_crimes_etl.loaders.traffic_crashes_vehicles_loader import TrafficCrashesVehiclesLoader
from chickago_crimes_etl.loaders.traffic_crashes_victims_agg_loader import TrafficCrashesVictimsAggLoader
from chickago_crimes_etl.processors.traffic_crashes_crashes import TrafficCrashesCrashesCSVProcessor
from chickago_crimes_etl.processors.traffic_crashes_people import TrafficCrashesPeopleCSVProcessor
from chickago_crimes_etl.processors.traffic_crashes_vehicles import TrafficCrashesVehiclesCSVProcessor
from chickago_crimes_etl.utils.run_utils import run_processor


@dag(
    dag_id="Chicago ETL",
    default_args={"retries": 1},
    params={
        "run_id": str(uuid.uuid4()),
    },
)
def process_chicago_etl():

    processors_matrix = []

    for processor_cls in (
        TrafficCrashesCrashesCSVProcessor,
        TrafficCrashesPeopleCSVProcessor,
        TrafficCrashesVehiclesCSVProcessor
    ):
        @task(task_id=processor_cls.__name__, op_kwargs={"run_id": "{{ params.run_id }}"})
        def runner(run_id: str):
            run_processor(processor_cls=processor_cls, run_id=run_id)
            return "OK"

        processors_matrix.append(runner)

    loader_matrix = []

    for loader_cls in (
        TrafficCrashesDataLoader,
        TrafficCrashesAccidentDateLoader,
        TrafficCrashesLocationsLoader,
        TrafficCrashesPoliceNotifiedDateLoader,
        TrafficCrashesVehiclesLoader,
        TrafficCrashesVictimsAggLoader,
    ):
        @task(task_id=loader_cls.__name__, op_kwargs={"run_id": "{{ params.run_id }}"})
        def runner(run_id: str):
            run_processor(loader_cls=loader_cls, run_id=run_id)
            return "OK"

        loader_matrix.append(runner)

    processors_matrix >> loader_matrix[0] >> loader_matrix[1:]


dag = process_chicago_etl()
