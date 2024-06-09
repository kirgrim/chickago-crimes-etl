from time import time


# PROCESSORS
# from chickago_crimes_etl.processors.traffic_crashes_crashes import TrafficCrashesCrashesCSVProcessor
# from chickago_crimes_etl.processors.traffic_crashes_people import TrafficCrashesPeopleCSVProcessor
# from chickago_crimes_etl.processors.traffic_crashes_vehicles import TrafficCrashesVehiclesCSVProcessor

# run_id = str(int(time()))

# processor_crashes = TrafficCrashesCrashesCSVProcessor()
# processor_vehicles = TrafficCrashesVehiclesCSVProcessor()
# processor_people = TrafficCrashesPeopleCSVProcessor()
#
# for processor in (processor_crashes, processor_vehicles, processor_people):
#     processor.run(run_id=run_id)

# LOADERS

run_id = '1717925457'

from chickago_crimes_etl.loaders.traffic_crashes_accident_time_loader import TrafficCrashesAccidentDateLoader
from chickago_crimes_etl.loaders.traffic_crashes_data_loader import TrafficCrashesLoader
from chickago_crimes_etl.loaders.traffic_crashes_locations_loader import TrafficCrashesLocationsLoader
from chickago_crimes_etl.loaders.traffic_crashes_accident_time_loader import TrafficCrashesAccidentDateLoader
from chickago_crimes_etl.loaders.traffic_crashes_accident_time_loader import TrafficCrashesAccidentDateLoader
from chickago_crimes_etl.loaders.traffic_crashes_accident_time_loader import TrafficCrashesAccidentDateLoader
