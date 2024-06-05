import os
from time import time

import pandas as pd
from abc import ABC, abstractmethod

from sqlalchemy.orm import sessionmaker

from .orm.models import Base, engine, TrafficAccidentVictimsInChicago


class TrafficCrashesLoader(ABC):
    FILE_PROCESSING_CHUNK_SIZE = int(os.getenv("FILE_PROCESSING_CHUNK_SIZE", "1000000"))
    BASE_PATH = os.getenv("BASE_PATH", "./")
    SessionFactory = None

    def __init__(self):
        if not self.SessionFactory:
            Base.metadata.create_all(engine)
            self.SessionFactory = sessionmaker(bind=engine)

    def run(self, run_id: str) -> str:
        i = 1
        print(f'starting processing of {run_id = !r}')
        with self.SessionFactory.begin() as session:
            existing_non_empty_traffic_accidents = session.query(
                TrafficAccidentVictimsInChicago.id_traffic_accident).where(getattr(TrafficAccidentVictimsInChicago,
                                                                                   self.fact_column_id).isnot(None)
                                                                           ).all()
            existing_non_empty_traffic_accidents = [x[0] for x in existing_non_empty_traffic_accidents]
        full_source_path = os.path.join(self.BASE_PATH, "dim_raw", run_id, self.csv_source_file)
        for chunk in pd.read_csv(full_source_path, chunksize=self.FILE_PROCESSING_CHUNK_SIZE, parse_dates=True):
            chunk = chunk.loc[~chunk['IdIncident'].isin(existing_non_empty_traffic_accidents)]
            if not chunk.empty:
                print(f'Running chunk #{i}')
                self.run_processing(data=chunk)
                i += 1

    def run_processing(self, data: pd.DataFrame):
        item_accident_ids = []
        items = []
        for i, row in data.iterrows():
            item_accident_ids.append(row['IdIncident'])
            items.append(self.create_single_item(row_data=row))
            self._store_extras(row)
        self._perform_bulk_insert(items=items, item_accident_ids=item_accident_ids)

    def _store_extras(self, row: dict):
        pass

    def _perform_bulk_insert(self, items: list, item_accident_ids: list[str]):
        with self.SessionFactory.begin() as session:
            try:
                session.bulk_save_objects(items)
                session.flush()
                inserted_ids = [item.id for item in items]
                self._update_fact_table_with_inserted_items(session=session,
                                                            item_accident_ids=item_accident_ids,
                                                            inserted_ids=inserted_ids)
                session.commit()
            except:
                # Rollback in case of error
                session.rollback()
                raise

    def _update_fact_table_with_inserted_items(self, session, item_accident_ids, inserted_ids):
        update_records = [{'id_traffic_accident': item_accident_ids[i],
                           self.fact_column_id: inserted_ids[i]}
                          for i in range(len(item_accident_ids))]
        session.bulk_update_mappings(TrafficAccidentVictimsInChicago, update_records)

    _step = None

    @staticmethod
    def gen_number():
        yield from range(1, 10**9, 100)

    @classmethod
    def _get_next_step(cls):
        if cls._step is None:
            cls._step = cls.gen_number()
        return next(cls._step)

    @classmethod
    def _get_ts_id(cls) -> int:
        res = cls._get_next_step() + int(time())
        return res

    @abstractmethod
    def create_single_item(self, row_data: dict):
        pass

    @property
    @abstractmethod
    def fact_column_id(self) -> str:
        pass

    @property
    @abstractmethod
    def csv_source_file(self) -> str:
        pass
