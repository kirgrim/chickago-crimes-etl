import io
import os
import sys
from collections import defaultdict
from time import time

import pandas as pd
from abc import ABC, abstractmethod

from sqlalchemy import func, insert
from sqlalchemy.orm import sessionmaker

from .orm.models import Base, engine, TrafficAccidentVictimsInChicago
from utils.base_etl_task import BaseETLTask


class TrafficCrashesLoader(BaseETLTask, ABC):
    FILE_PROCESSING_CHUNK_SIZE = int(os.getenv("FILE_PROCESSING_CHUNK_SIZE", "1000000"))
    SessionFactory = None
    pivot_table_mapping = {}

    def __init__(self):
        if not self.SessionFactory:
            Base.metadata.create_all(engine)
            self.SessionFactory = sessionmaker(bind=engine)
        self.max_id = -1

    def run(self, run_id: str) -> str:
        i = 1
        print(f'starting processing of {run_id = !r}')
        with self.SessionFactory.begin() as session:
            existing_non_empty_traffic_accidents = session.query(
                TrafficAccidentVictimsInChicago.idAccidentTraffic).where(getattr(TrafficAccidentVictimsInChicago,
                                                                                 self.fact_column_id).isnot(None)
                                                                           ).all()
            existing_non_empty_traffic_accidents = [x[0] for x in existing_non_empty_traffic_accidents]
            self.max_id = self.get_max_id(session=session)
        full_source_path = os.path.join(self.DIM_FILES_DIR, run_id, self.csv_source_file)
        for chunk in pd.read_csv(full_source_path, chunksize=self.FILE_PROCESSING_CHUNK_SIZE, parse_dates=True):
            chunk = chunk.loc[~chunk['IdIncident'].isin(existing_non_empty_traffic_accidents)]
            if not chunk.empty:
                print(f'Running chunk #{i}')
                self.run_processing(data=chunk, run_id=run_id)
                i += 1

    def get_max_id(self, session) -> int:
        return session.query(func.max(self.target_table.id)).first()[0] or 0

    def run_processing(self, data: pd.DataFrame, run_id: str):
        data = self.clean_data(data)
        data = self.store_id_mapping(data=data, run_id=run_id)
        data_records = data.to_dict('records')
        try:
            self.execute_bulk_insert(data_records)
        except Exception as e:
            print(f'Failed to execute bulk insert ({e}) trying with executemany', file=sys.stderr)
            self.execute_in_batches(data_records, batch_size=150)

    def clean_data(self, data: pd.DataFrame) -> pd.DataFrame:
        return data

    def execute_bulk_insert(self, data_records):
        with engine.begin() as conn:
            conn.execute(insert(self.target_table), data_records)

    def execute_in_batches(self, data_records, batch_size):
        with engine.begin() as conn:
            processed = 0
            for i in range(0, len(data_records), batch_size):
                batch = data_records[i:i + batch_size]
                stmt = insert(self.target_table).values(batch)
                try:
                    conn.execute(stmt)
                    processed += batch_size
                    print('\r' + str(round(i / len(data_records) * 100, 1)) + '% complete', end='')
                    sys.stdout.flush()
                except Exception as e:
                    print(f"Error inserting batch {i // batch_size}: {e}")

    def store_id_mapping(self, data, run_id: str):
        data[self.dim_column_id] = range(self.max_id + 1, self.max_id + len(data) + 1)
        map_data = data.filter(items=[self.dim_column_id, 'IdIncident'])
        map_data.to_csv(self.get_dim_to_fact_mapping_path(run_id=run_id), mode='a', index=False)
        self.max_id += len(data)
        data = data[data.columns[~data.columns.isin(['IdIncident'])]]
        return data

    @property
    @abstractmethod
    def fact_column_id(self) -> str:
        pass

    @property
    @abstractmethod
    def csv_source_file(self) -> str:
        pass

    @property
    @abstractmethod
    def target_table(self):
        pass

    @property
    def dim_column_id(self) -> str:
        return self.target_table.id.expression.name

    def get_mapping_dir(self, run_id: str):
        return os.path.join(self.DIM_FILES_DIR, run_id, "id_mapping")

    def get_dim_to_fact_mapping_path(self, run_id: str) -> str:
        mapping_dir = self.get_mapping_dir(run_id=run_id)
        os.makedirs(mapping_dir, exist_ok=True)
        return os.path.join(mapping_dir, self.csv_source_file)
