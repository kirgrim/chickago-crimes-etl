import uuid

from airflow.decorators import dag, task

from loaders.traffic_crashes_accident_time_loader import TrafficCrashesAccidentDateLoader
from loaders.traffic_crashes_data_loader import TrafficCrashesDataLoader
from loaders.traffic_crashes_locations_loader import TrafficCrashesLocationsLoader
from loaders.traffic_crashes_police_notified_date_loader import \
    TrafficCrashesPoliceNotifiedDateLoader
from loaders.traffic_crashes_vehicles_loader import TrafficCrashesVehiclesLoader
from loaders.traffic_crashes_victims_agg_loader import TrafficCrashesVictimsAggLoader
from processors.traffic_crashes_crashes import TrafficCrashesCrashesCSVProcessor
from processors.traffic_crashes_people import TrafficCrashesPeopleCSVProcessor
from processors.traffic_crashes_vehicles import TrafficCrashesVehiclesCSVProcessor
from utils.run_utils import run_processor


@dag(
    dag_id="chicago-etl",
    default_args={"retries": 1},
    params={
        # TODO: this won't change - take jobId from **kwargs
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
        runner(loader_cls=loader_cls, run_id="{{ params.run_id }}")
        loader_matrix.append(runner)
    # TODO: this won't work - make it "partial"
    processors_matrix >> [loader_matrix[0]] >> loader_matrix[1:]


dag = process_chicago_etl()
