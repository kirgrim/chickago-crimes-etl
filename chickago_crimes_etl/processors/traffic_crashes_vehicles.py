import os
import pandas as pd

from .base import TrafficCrashesCSVProcessor


class TrafficCrashesVehiclesCSVProcessor(TrafficCrashesCSVProcessor):
    @property
    def csv_source_path(self) -> str:
        return os.getenv("TRAFFIC_CRASHES_VEHICLES_SOURCE_PATH")

    def run_processing(self, data: pd.DataFrame, destination_path: str):
        pass