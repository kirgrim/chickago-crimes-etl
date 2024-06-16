import pandas as pd

from .base import TrafficCrashesLoader
from .orm.models import TrafficAccidentVehicle


class TrafficCrashesVehiclesLoader(TrafficCrashesLoader):

    @property
    def target_table(self):
        return TrafficAccidentVehicle

    @property
    def fact_column_id(self) -> str:
        return 'idTrafficAccidentVehicle'

    @property
    def csv_source_file(self) -> str:
        return 'traffic_vehicle_dim.csv'

    def clean_data(self, data):
        data['vehicleYear'] = data['vehicleYear'].astype(int)
        data['vehicleYear'] = data['vehicleYear'].apply(lambda x: x if x >= 0 else 0)
        return data
