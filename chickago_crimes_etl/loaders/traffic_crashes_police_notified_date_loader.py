from .traffic_crashes_time_loader import TrafficCrashesDateLoader


class TrafficCrashesPoliceNotifiedDateLoader(TrafficCrashesDateLoader):

    @property
    def fact_column_id(self) -> str:
        return 'idTrafficAccidentPoliceNotified'
