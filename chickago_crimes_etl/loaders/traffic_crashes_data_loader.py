from .base import TrafficCrashesLoader
from .orm.models import TrafficAccident


class TrafficCrashesDataLoader(TrafficCrashesLoader):

    @property
    def target_table(self):
        return TrafficAccident

    @property
    def fact_column_id(self) -> str:
        return "idTrafficAccident"

    @property
    def csv_source_file(self) -> str:
        return "accident_traffic_dim.csv"

    def store_id_mapping(self, data, run_id: str):
        return data.rename(columns={'IdIncident': 'idTrafficAccident'})

    def get_max_id(self, session):
        return -1
