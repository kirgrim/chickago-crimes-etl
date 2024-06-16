import os
import pandas as pd

from .base import TrafficCrashesCSVProcessor


class TrafficCrashesVehiclesCSVProcessor(TrafficCrashesCSVProcessor):

    def run_processing(self, data: pd.DataFrame, destination_path: str):
        self._populate_vehicle_dim_csv(data=data,
                                       destination_path=destination_path)

    def _populate_vehicle_dim_csv(self, data: pd.DataFrame, destination_path: str):
        victims_agg_dim_filename = 'traffic_vehicle_dim.csv'
        destination_path = os.path.join(destination_path, victims_agg_dim_filename)
        # removing multiple-car accidents
        data['VEHICLE_YEAR'] = data['VEHICLE_YEAR'].fillna(0)
        data['VEHICLE_YEAR'] = data['VEHICLE_YEAR'].astype(int)
        data = data.fillna("UNKNOWN")
        vehicle_columns = {'CRASH_RECORD_ID': 'IdIncident',
                           'MAKE': 'vehicleMake',
                           'MODEL': 'vehicleModel',
                           'VEHICLE_YEAR': 'vehicleYear',
                           'VEHICLE_TYPE': 'vehicleType',
                           'VEHICLE_USE': 'vehicleUse',
                           'MANEUVER': 'maneuver'}
        self._populate_to_csv(data=data, destination_path=destination_path, columns_mapping=vehicle_columns)
