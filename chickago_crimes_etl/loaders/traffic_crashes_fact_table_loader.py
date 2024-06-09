import glob
import pandas as pd

from .base import TrafficCrashesLoader
from .orm.models import TrafficAccidentVictimsInChicago


class TrafficCrashesFactTableLoader(TrafficCrashesLoader):

    def __init__(self):
        super().__init__()
        self.current_dfs = None
        self.FILE_PROCESSING_CHUNK_SIZE = 10**7

    @property
    def fact_column_id(self) -> str:
        return "idTrafficAccident"

    @property
    def csv_source_file(self) -> str:
        return "fact_table_mapping.csv"

    @property
    def target_table(self):
        return TrafficAccidentVictimsInChicago

    def get_max_id(self, session) -> int:
        return -1

    def store_id_mapping(self, data, run_id: str):
        return data

    def clean_data(self, data: pd.DataFrame) -> pd.DataFrame:
        data = data.merge(self.current_dfs, left_on="idTrafficAccident", right_on="IdIncident", how="outer")
        data = data[data.columns[~data.columns.isin(['IdIncident'])]]
        cols = ['idTrafficAccidentPoliceNotified',
                'idTrafficAccidentVehicle',
                'idTrafficAccidentVictimsAgg',
                'idTrafficAccidentTime',
                'idTrafficAccidentLocation',]
        data[cols] = data[cols].fillna(0).astype(int)
        data = data.drop_duplicates(subset=['idTrafficAccident'], keep="first")
        return data

    @property
    def incident_id_key(self):
        return 'idTrafficAccident'

    def merge_dim_dfs(self, run_id: str):
        csv_files = glob.glob(f'{self.get_mapping_dir(run_id=run_id)}/*.csv')
        dfs = []
        for file in csv_files:
            df = pd.read_csv(file)
            dfs.append(df)
        merged_df = dfs[0]
        for df in dfs[1:]:
            merged_df = merged_df.merge(df, on="IdIncident", how='outer')
        return merged_df

    def run(self, run_id: str) -> str:
        self.current_dfs = self.merge_dim_dfs(run_id)
        return super().run(run_id=run_id)
