import os
import uuid

from chickago_crimes_etl.processors.traffic_crashes_crashes import TrafficCrashesCrashesCSVProcessor

run_id = str(uuid.uuid4())


processor = TrafficCrashesCrashesCSVProcessor()

processor.run(run_id=run_id)
