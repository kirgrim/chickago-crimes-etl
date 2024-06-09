from .traffic_crashes_time_loader import TrafficCrashesDateLoader


class TrafficCrashesAccidentDateLoader(TrafficCrashesDateLoader):

    @property
    def fact_column_id(self) -> str:
        return 'idTrafficAccidentTime'
