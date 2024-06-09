import os
import pandas as pd
from abc import ABC, abstractmethod

from utils.base_etl_task import BaseETLTask


class TrafficCrashesCSVProcessor(BaseETLTask, ABC):

    FILE_PROCESSING_CHUNK_SIZE = int(os.getenv("FILE_PROCESSING_CHUNK_SIZE", "1000000"))

    def run(self, run_id: str) -> str:
        destination_path = os.path.join(self.DIM_FILES_DIR, run_id)
        try:
            os.makedirs(destination_path, exist_ok=True)
        except FileExistsError:
            pass
        print(f'starting processing of {run_id = !r}')
        for i, chunk in enumerate(pd.read_csv(self.csv_source_path, chunksize=self.FILE_PROCESSING_CHUNK_SIZE, parse_dates=True)):
            print(f'Running chunk #{i+1}')
            self.run_processing(data=chunk, destination_path=destination_path)

    @property
    @abstractmethod
    def csv_source_path(self) -> str:
        pass

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
