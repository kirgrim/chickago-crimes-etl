from abc import ABC

from .base import TrafficCrashesLoader
from .orm.models import TrafficAccidentTime


class TrafficCrashesDateLoader(TrafficCrashesLoader, ABC):

    @property
    def dim_column_id(self) -> str:
        return 'timeId'

    @property
    def target_table(self):
        return TrafficAccidentTime

    @property
    def csv_source_file(self) -> str:
        return 'accident_time_dim.csv'

    def store_id_mapping(self, data, run_id: str):
        data = super().store_id_mapping(data, run_id)
        data = data.rename(columns={self.dim_column_id: 'timeId'})
        return data
