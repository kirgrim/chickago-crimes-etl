from .base import TrafficCrashesLoader
from .orm.models import TrafficAccidentLocation


class TrafficCrashesLocationsLoader(TrafficCrashesLoader):

    @property
    def target_table(self):
        return TrafficAccidentLocation

    @property
    def fact_column_id(self) -> str:
        return 'idTrafficAccidentLocation'

    @property
    def csv_source_file(self) -> str:
        return 'accident_location_dim.csv'
