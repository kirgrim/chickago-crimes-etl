import os
import pandas as pd
from abc import ABC, abstractmethod


class TrafficCrashesCSVProcessor(ABC):

    FILE_PROCESSING_CHUNK_SIZE = int(os.getenv("FILE_PROCESSING_CHUNK_SIZE", "100000"))

    def run(self, run_id: str) -> str:
        destination_path = os.path.join(self.processing_result_store_dir, run_id)
        os.mkdir(destination_path)
        i = 1
        for chunk in pd.read_csv(self.csv_source_path, chunksize=self.FILE_PROCESSING_CHUNK_SIZE, parse_dates=True):
            print(f'Running chunk #{i}')
            self.run_processing(data=chunk, destination_path=destination_path)
            i += 1

    @property
    @abstractmethod
    def csv_source_path(self) -> str:
        pass

    @property
    def processing_result_store_dir(self) -> str:
        return os.getenv("RESULT_STORE_BASE_PATH", os.path.join(os.path.dirname(__file__), 'result', 'dim_raw'))

    @abstractmethod
    def run_processing(self, data: pd.DataFrame, destination_path: str):
        pass
