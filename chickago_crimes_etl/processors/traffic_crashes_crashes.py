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
        print('.', end='')
        self._populate_location_dim_csv(data=data,
                                        destination_path=destination_path)
        print('.', end='')
        self._populate_accident_time_dim_csv(data=data,
                                             destination_path=destination_path)
        print('.', end='')
        self._populate_police_notified_date_dim_csv(data=data,
                                                    destination_path=destination_path)
        print('. DONE', end='\n')

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
                                    'INJURIES_TOTAL': 'InjuriesTotal',
                                    'INJURIES_FATAL': 'InjuriesFatal',
                                    'PRIM_CONTRIBUTORY_CAUSE': 'Cause'
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
            'YEAR': 'Year',
            'MONTH': 'Month',
            'MONTH_NAME': 'MonthName',
            'DAY_OF_WEEK': 'WeekDay',
            'DAY_OF_WEEK_NAME': 'WeekDayName',
            'DAY': 'Day',
            'HOUR': 'Hour',
            'MINUTE': 'Minute'
        }
        data = self._decompose_date_columns(data=data, source_column='CRASH_DATE')
        self._populate_to_csv(data=data, destination_path=destination_path, columns_mapping=accident_time_columns)

    def _populate_police_notified_date_dim_csv(self, data: pd.DataFrame, destination_path: str):
        accident_time_dim_filename = 'police_notified_date_dim.csv'
        destination_path = os.path.join(destination_path, accident_time_dim_filename)
        accident_time_columns = {
            'CRASH_RECORD_ID': 'IdIncident',
            'DATE_POLICE_NOTIFIED': 'IncidentDate',
            'YEAR': 'Year',
            'MONTH': 'Month',
            'MONTH_NAME': 'MonthName',
            'DAY_OF_WEEK': 'WeekDay',
            'DAY_OF_WEEK_NAME': 'WeekDayName',
            'DAY': 'Day',
            'HOUR': 'Hour',
            'MINUTE': 'Minute'
        }
        data = self._decompose_date_columns(data=data, source_column='DATE_POLICE_NOTIFIED')
        self._populate_to_csv(data=data, destination_path=destination_path, columns_mapping=accident_time_columns)

    @staticmethod
    def _decompose_date_columns(data, source_column: str):
        date_series = data[source_column].apply(lambda x: datetime.datetime.strptime(x, "%m/%d/%Y %I:%M:%S %p"))
        data['YEAR'] = date_series.apply(lambda x: x.year)
        data['MONTH'] = date_series.apply(lambda x: x.month)
        data['MONTH_NAME'] = date_series.apply(lambda x: x.strftime("%B"))
        data['DAY_OF_WEEK'] = date_series.apply(lambda x: x.weekday())
        data['DAY_OF_WEEK_NAME'] = date_series.apply(lambda x: x.strftime("%A"))
        data['DAY'] = date_series.apply(lambda x: x.day)
        data['HOUR'] = date_series.apply(lambda x: x.hour)
        data['MINUTE'] = date_series.apply(lambda x: x.minute)
        return data
