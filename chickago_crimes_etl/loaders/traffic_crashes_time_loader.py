from abc import ABC

from .base import TrafficCrashesLoader
from .orm.models import TrafficAccidentTime


class TrafficCrashesDateLoader(TrafficCrashesLoader, ABC):

    @property
    def dim_column_id(self) -> str:
        return self.fact_column_id

    @property
    def target_table(self):
        return TrafficAccidentTime

    def store_id_mapping(self, data, run_id: str):
        data = super().store_id_mapping(data, run_id)
        data = data.rename(columns={self.fact_column_id: "timeId"})
        return data
