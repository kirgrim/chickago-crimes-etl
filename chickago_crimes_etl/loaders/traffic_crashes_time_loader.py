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

    def store_id_mapping(self, data, run_id: str):
        df_copy = data.copy()
        df_copy.rename(columns={self.dim_column_id: self.fact_column_id}, inplace=True)
        data = super().store_id_mapping(data, run_id)
        return data
