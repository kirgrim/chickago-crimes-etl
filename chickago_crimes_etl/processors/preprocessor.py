import os

import pandas as pd
import functools as ft


class TrafficCrashesPreprocessor:

    @property
    def csv_source_path(self) -> str:
        return os.getenv("TRAFFIC_CRASHES_SRC_PATH")

    def get_dest_file_path(self, run_id: str) -> str:
        os.makedirs(os.path.join(self.csv_source_path, run_id), exist_ok=True)
        return os.path.join(self.csv_source_path, run_id, "traffic_crashes_joined.csv")

    @property
    def crashes_file_path(self) -> str:
        return os.path.join(self.csv_source_path, "Traffic_Crashes_Crashes.csv")

    @property
    def people_file_path(self) -> str:
        return os.path.join(self.csv_source_path, "Traffic_Crashes_People.csv")

    @property
    def vehicles_file_path(self) -> str:
        return os.path.join(self.csv_source_path, "Traffic_Crashes_Vehicles.csv")

    def get_crashes_df(self) -> pd.DataFrame:
        df = pd.read_csv(self.crashes_file_path)
        df = df.drop_duplicates(subset=['CRASH_RECORD_ID'], keep="first")
        return df

    def get_vehicles_df(self) -> pd.DataFrame:
        df = pd.read_csv(self.vehicles_file_path)
        df = df.loc[df['UNIT_TYPE'] == 'DRIVER']
        df = df.drop_duplicates(subset=['CRASH_RECORD_ID'], keep="first")
        return df

    def get_agg_people_df(self):
        # passenger type aggregation
        df = pd.read_csv(self.people_file_path)

        passenger_type_aggregation_df = df.filter(items=['CRASH_RECORD_ID', 'PERSON_TYPE']).loc[
            df['PERSON_TYPE'].isin(['PEDESTRIAN', 'DRIVER', 'PASSENGER', ])]
        passenger_type_aggregation_df = passenger_type_aggregation_df.groupby(
            by=['CRASH_RECORD_ID', 'PERSON_TYPE']).size().unstack(fill_value=0)
        passenger_type_aggregation_df = passenger_type_aggregation_df.rename(
            columns={'PEDESTRIAN': 'NUM_PEDESTRIAN_VICTIMS',
                     'DRIVER': 'NUM_DRIVER_VICTIMS',
                     'PASSENGER': 'NUM_PASSENGER_VICTIMS'}).reset_index()

        # gender aggregation
        data_grouped_person_type_df = df.filter(items=['CRASH_RECORD_ID', 'SEX']).loc[df['SEX'].isin(['M', 'F', ])]
        data_grouped_person_type_df = data_grouped_person_type_df.groupby(by=['CRASH_RECORD_ID', 'SEX']).size().unstack(
            fill_value=0)
        data_grouped_person_type_df = data_grouped_person_type_df.rename(
            columns={'M': 'NUM_MALES', 'F': 'NUM_FEMALES'}).reset_index()

        bins = [0, 18, 60, float('inf')]
        labels = ['CHILDREN', 'ADULTS', 'SENIORS']

        data_grouped_by_age_group_df = df.filter(items=['CRASH_RECORD_ID', 'AGE'])
        data_grouped_by_age_group_df['AGE_GROUP'] = pd.cut(df['AGE'], bins=bins, labels=labels, right=False)
        data_grouped_by_age_group_df = data_grouped_by_age_group_df.groupby(
            ['CRASH_RECORD_ID', 'AGE_GROUP']).size().unstack(fill_value=0)
        data_grouped_by_age_group_df = data_grouped_by_age_group_df.rename(
            columns=lambda x: f'NUM_{x.upper()}').reset_index()

        df_merged = ft.reduce(lambda left, right: pd.merge(left, right, on=['CRASH_RECORD_ID']),
                              [passenger_type_aggregation_df,
                               data_grouped_person_type_df,
                               data_grouped_by_age_group_df])
        df_merged = df_merged.fillna(0)
        return df_merged

    def run(self, run_id: str):
        crashes_df = self.get_crashes_df()
        vehicles_df = self.get_vehicles_df()
        people_df = self.get_agg_people_df()
        df_final = ft.reduce(lambda left, right: pd.merge(left, right, on='CRASH_RECORD_ID'), (
            crashes_df, vehicles_df, people_df,
        )).rename(columns={'CRASH_DATE_x': 'CRASH_DATE'})
        df_final.to_csv(self.get_dest_file_path(run_id), index=False)
        return 'OK'
