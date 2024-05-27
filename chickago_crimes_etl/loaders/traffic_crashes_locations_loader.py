from .base import TrafficCrashesLoader
from .orm.models import TrafficAccidentLocation


class TrafficCrashesLocationsLoader(TrafficCrashesLoader):

    def create_single_item(self, row_data: dict):
        return TrafficAccidentLocation(id=self._get_ts_id(),
                                       latitude=row_data['Latitude'],
                                       longitude=row_data['Longitude'],
                                       crash_location=row_data['CrashLocation'],
                                       street_no=row_data['StreetNo'],
                                       street_name=row_data['StreetName'])

    @property
    def fact_column_id(self) -> str:
        return 'id_traffic_accident_location'

    @property
    def csv_source_file(self) -> str:
        return 'accident_location_dim.csv'
