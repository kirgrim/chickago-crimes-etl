from .base import TrafficCrashesLoader
from .orm.models import TrafficAccidentVehicle


class TrafficCrashesVehiclesLoader(TrafficCrashesLoader):

    def create_single_item(self, row_data: dict):
        return TrafficAccidentVehicle(id=self._get_ts_id(),
                                      vehicle_make=row_data['VehicleMake'],
                                      vehicle_model=row_data['VehicleModel'],
                                      vehicle_year=row_data['VehicleYear'],
                                      vehicle_type=row_data['VehicleType'],
                                      vehicle_use=row_data['VehicleUse'],
                                      maneuver=row_data['Maneuver'])

    @property
    def fact_column_id(self) -> str:
        return 'id_traffic_accident_location'

    @property
    def csv_source_file(self) -> str:
        return 'accident_location_dim.csv'
