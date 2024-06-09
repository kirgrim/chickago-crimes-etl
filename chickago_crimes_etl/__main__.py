from time import time

run_id = str(int(time()))

# PROCESSORS
from chickago_crimes_etl.processors.traffic_crashes_crashes import TrafficCrashesCrashesCSVProcessor
from chickago_crimes_etl.processors.traffic_crashes_people import TrafficCrashesPeopleCSVProcessor
from chickago_crimes_etl.processors.traffic_crashes_vehicles import TrafficCrashesVehiclesCSVProcessor

from chickago_crimes_etl.utils.run_utils import run_processor

for processor in (
    TrafficCrashesCrashesCSVProcessor,
    TrafficCrashesPeopleCSVProcessor,
    TrafficCrashesVehiclesCSVProcessor,
):
    run_processor(processor_cls=processor, run_id=run_id)


# LOADERS

from chickago_crimes_etl.loaders.traffic_crashes_accident_time_loader import TrafficCrashesAccidentDateLoader
from chickago_crimes_etl.loaders.traffic_crashes_data_loader import TrafficCrashesDataLoader
from chickago_crimes_etl.loaders.traffic_crashes_locations_loader import TrafficCrashesLocationsLoader
from chickago_crimes_etl.loaders.traffic_crashes_police_notified_date_loader import TrafficCrashesPoliceNotifiedDateLoader
from chickago_crimes_etl.loaders.traffic_crashes_vehicles_loader import TrafficCrashesVehiclesLoader
from chickago_crimes_etl.loaders.traffic_crashes_victims_agg_loader import TrafficCrashesVictimsAggLoader

from chickago_crimes_etl.utils.run_utils import run_loader

for loader in (
    TrafficCrashesDataLoader,
    TrafficCrashesAccidentDateLoader,
    TrafficCrashesLocationsLoader,
    TrafficCrashesPoliceNotifiedDateLoader,
    TrafficCrashesVehiclesLoader,
    TrafficCrashesVictimsAggLoader,
):
    run_loader(loader_cls=loader, run_id=run_id)
