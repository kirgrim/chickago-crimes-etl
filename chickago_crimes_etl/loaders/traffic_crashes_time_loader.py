from abc import ABC

from .base import TrafficCrashesLoader
from .orm.models import TrafficAccidentTime


class TrafficCrashesDateLoader(TrafficCrashesLoader, ABC):

    def create_single_item(self, row_data: dict):
        return TrafficAccidentTime(id=self._get_ts_id(),
                                   date=row_data['IncidentDate'],
                                   year=row_data['Year'],
                                   month=int(row_data['Month']),
                                   month_name=row_data['MonthName'],
                                   week_day=int(row_data['WeekDay']),
                                   week_day_name=row_data['WeekDayName'],
                                   day=int(row_data['Day']),
                                   hour=int(row_data['Hour']),
                                   minute=int(row_data['Minute']),)
