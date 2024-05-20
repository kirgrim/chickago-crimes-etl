import datetime
import os
import pandas as pd

from .base import TrafficCrashesCSVProcessor


class TrafficCrashesCrashesCSVProcessor(TrafficCrashesCSVProcessor):
    @property
    def csv_source_path(self) -> str:
        return os.getenv("TRAFFIC_CRASHES_CRASHES_SOURCE_PATH")

    def run_processing(self, data: pd.DataFrame, destination_path: str):
        self._populate_accident_traffic_dim_csv(data=data,
                                                destination_path=destination_path)
        print('.')
        self._populate_location_dim_csv(data=data,
                                        destination_path=destination_path)
        print('.')
        self._populate_accident_time_dim_csv(data=data,
                                             destination_path=destination_path)
        print('.')

    def _populate_accident_traffic_dim_csv(self, data: pd.DataFrame, destination_path: str):
        accident_traffic_dim_filename = 'accident_traffic_dim.csv'
        destination_path = os.path.join(destination_path, accident_traffic_dim_filename)
        accident_traffic_columns = {'CRASH_RECORD_ID': 'IdIncident',
                                    'WEATHER_CONDITION': 'WeatherCondition',
                                    'LIGHTING_CONDITION': 'LightingCondition',
                                    'ROADWAY_SURFACE_COND': 'RoadwaySurfaceCond',
                                    'DAMAGE': 'Damage',
                                    'POSTED_SPEED_LIMIT': 'PostedSpeedLimit',
                                    'CRASH_TYPE': 'CrashType',
                                    'DATE_POLICE_NOTIFIED': 'IdDatePoliceNotified',
                                    'INJURIES_TOTAL': 'InjuriesTotal',
                                    'INJURIES_FATAL': 'InjuriesFatal',
                                    }
        data['INJURIES_TOTAL'] = data['INJURIES_TOTAL'].fillna(0).astype(int)
        data['INJURIES_FATAL'] = data['INJURIES_FATAL'].fillna(0).astype(int)
        self._populate_to_csv(data=data, destination_path=destination_path, columns_mapping=accident_traffic_columns)

    def _populate_location_dim_csv(self, data: pd.DataFrame, destination_path: str):
        location_columns = {
            'CRASH_RECORD_ID': 'IdIncident',
            'LATITUDE': 'Latitude',
            'LONGITUDE': 'Longitude',
            'LOCATION': 'CrashLocation',
            'STREET_NO': 'StreetNo',
            'STREET_NAME': 'StreetName',
        }
        accident_location_dim_filename = 'accident_location_dim.csv'
        destination_path = os.path.join(destination_path, accident_location_dim_filename)
        self._populate_to_csv(data=data, destination_path=destination_path, columns_mapping=location_columns)

    def _populate_accident_time_dim_csv(self, data: pd.DataFrame, destination_path: str):
        accident_time_dim_filename = 'accident_time_dim.csv'
        destination_path = os.path.join(destination_path, accident_time_dim_filename)
        accident_time_columns = {
            'CRASH_RECORD_ID': 'IdIncident',
            'CRASH_DATE': 'IncidentDate',
            'CRASH_YEAR': 'Year',
            'CRASH_MONTH': 'Month',
            'CRASH_MONTH_NAME': 'MonthName',
            'CRASH_DAY_OF_WEEK': 'WeekDay',
            'CRASH_DAY_OF_WEEK_NAME': 'WeekDayName',
            'CRASH_DAY': 'Day',
            'CRASH_HOUR': 'Hour',
            'CRASH_MINUTE': 'Minute'
        }
        date_series = data["CRASH_DATE"].apply(lambda x: datetime.datetime.strptime(x, "%m/%d/%Y %I:%M:%S %p"))
        data['CRASH_YEAR'] = date_series.apply(lambda x: x.year)
        data['CRASH_DAY'] = date_series.apply(lambda x: x.day)
        data['CRASH_MINUTE'] = date_series.apply(lambda x: x.minute)
        data['CRASH_MONTH_NAME'] = date_series.apply(lambda x: x.strftime("%B"))
        data['CRASH_DAY_OF_WEEK_NAME'] = date_series.apply(lambda x: x.strftime("%A"))

        self._populate_to_csv(data=data, destination_path=destination_path, columns_mapping=accident_time_columns)
