from time import time

from chickago_crimes_etl.processors.traffic_crashes_crashes import TrafficCrashesCrashesCSVProcessor
from chickago_crimes_etl.processors.traffic_crashes_people import TrafficCrashesPeopleCSVProcessor
from chickago_crimes_etl.processors.traffic_crashes_vehicles import TrafficCrashesVehiclesCSVProcessor

run_id = str(int(time()))

processor_crashes = TrafficCrashesCrashesCSVProcessor()
processor_vehicles = TrafficCrashesVehiclesCSVProcessor()
processor_people = TrafficCrashesPeopleCSVProcessor()

for processor in (processor_crashes, processor_vehicles, processor_people):
    processor.run(run_id=run_id)
