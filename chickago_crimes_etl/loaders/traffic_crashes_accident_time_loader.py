from .traffic_crashes_time_loader import TrafficCrashesDateLoader


class TrafficCrashesAccidentDateLoader(TrafficCrashesDateLoader):

    @property
    def fact_column_id(self) -> str:
        return 'idTrafficAccidentTime'

    @property
    def csv_source_file(self) -> str:
        return 'accident_time_dim.csv'
