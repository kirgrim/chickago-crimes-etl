from .traffic_crashes_time_loader import TrafficCrashesDateLoader


class TrafficCrashesPoliceNotifiedDateLoader(TrafficCrashesDateLoader):

    @property
    def fact_column_id(self) -> str:
        return 'id_traffic_accident_police_notified'

    @property
    def csv_source_file(self) -> str:
        return 'police_notified_date_dim.csv'
