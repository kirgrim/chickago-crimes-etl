from airflow.decorators import dag, task

from loaders.traffic_crashes_accident_time_loader import TrafficCrashesAccidentDateLoader
from loaders.traffic_crashes_data_loader import TrafficCrashesDataLoader
from loaders.traffic_crashes_locations_loader import TrafficCrashesLocationsLoader
from loaders.traffic_crashes_police_notified_date_loader import \
    TrafficCrashesPoliceNotifiedDateLoader
from loaders.traffic_crashes_vehicles_loader import TrafficCrashesVehiclesLoader
from loaders.traffic_crashes_victims_agg_loader import TrafficCrashesVictimsAggLoader
from loaders.traffic_crashes_fact_table_loader import TrafficCrashesFactTableLoader
from processors.traffic_crashes_crashes import TrafficCrashesCrashesCSVProcessor
from processors.traffic_crashes_people import TrafficCrashesPeopleCSVProcessor
from processors.traffic_crashes_vehicles import TrafficCrashesVehiclesCSVProcessor
from utils.run_utils import run_processor, run_loader


@dag(
    dag_id="chicago-etl",
    default_args={"retries": 0},
)
def process_chicago_etl():

    processors_matrix = []

    @task
    def processor_runner(processor_cls, **kwargs):
        """This is a function that will run within the DAG execution"""
        run_processor(processor_cls=processor_cls, run_id=kwargs['dag_run'].run_id)
        return "OK"

    for processor_cls in (
        TrafficCrashesCrashesCSVProcessor,
        TrafficCrashesPeopleCSVProcessor,
        TrafficCrashesVehiclesCSVProcessor
    ):
        processor_task = processor_runner.override(task_id=processor_cls.__name__)(processor_cls=processor_cls)

        processors_matrix.append(processor_task)

    @task
    def loader_runner(loader_cls, **kwargs):
        """This is a function that will run within the DAG execution"""
        run_loader(loader_cls=loader_cls, run_id=kwargs['dag_run'].run_id)
        return "OK"

    loader_matrix = []

    for loader_cls in (
        TrafficCrashesDataLoader,
        TrafficCrashesAccidentDateLoader,
        TrafficCrashesLocationsLoader,
        TrafficCrashesPoliceNotifiedDateLoader,
        TrafficCrashesVehiclesLoader,
        TrafficCrashesVictimsAggLoader,
        TrafficCrashesFactTableLoader,
    ):
        loader_task = loader_runner.override(task_id=loader_cls.__name__)(loader_cls=loader_cls)
        loader_matrix.append(loader_task)

    final_task = loader_matrix.pop()

    for processor_task in processors_matrix:
        for loader_task in loader_matrix:
            processor_task >> loader_task

    for loader_task in loader_matrix:
        loader_task >> final_task


dag = process_chicago_etl()
