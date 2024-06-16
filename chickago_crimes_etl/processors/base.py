import os
import pandas as pd
from abc import ABC, abstractmethod

from utils.base_etl_task import BaseETLTask


class TrafficCrashesCSVProcessor(BaseETLTask, ABC):

    FILE_PROCESSING_CHUNK_SIZE = int(os.getenv("FILE_PROCESSING_CHUNK_SIZE", "1000000"))
    TRAFFIC_CRASHES_SRC_PATH = os.getenv("TRAFFIC_CRASHES_SRC_PATH")

    def run(self, run_id: str) -> str:
        destination_path = os.path.join(self.DIM_FILES_DIR, run_id)
        os.makedirs(destination_path, exist_ok=True)
        print(f'starting processing of {run_id = !r}')
        for i, chunk in enumerate(pd.read_csv(self.get_csv_source_path(run_id=run_id),
                                              chunksize=self.FILE_PROCESSING_CHUNK_SIZE,
                                              parse_dates=True)):
            print(f'Running chunk #{i+1}')
            self.run_processing(data=chunk, destination_path=destination_path)

    def get_csv_source_path(self, run_id: str) -> str:
        return os.path.join(self.TRAFFIC_CRASHES_SRC_PATH, run_id, 'traffic_crashes_joined.csv')

    @abstractmethod
    def run_processing(self, data: pd.DataFrame, destination_path: str):
        pass

    @staticmethod
    def _populate_to_csv(data: pd.DataFrame, destination_path: str, columns_mapping: dict[str, str] = None):
        if columns_mapping:
            data = data.filter(items=list(columns_mapping)).rename(columns=columns_mapping)
        try:
            if os.path.getsize(destination_path):
                header = False
            else:
                header = True
        except:
            header = True
        data.to_csv(destination_path, mode='a', index=False, header=header)
