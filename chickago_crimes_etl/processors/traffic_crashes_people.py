import os
import pandas as pd
from functools import reduce

from .base import TrafficCrashesCSVProcessor


class TrafficCrashesPeopleCSVProcessor(TrafficCrashesCSVProcessor):
    @property
    def csv_source_path(self) -> str:
        return os.getenv("TRAFFIC_CRASHES_PEOPLE_SOURCE_PATH")

    def run_processing(self, data: pd.DataFrame, destination_path: str):
        self._populate_people_dim_csv(data=data,
                                      destination_path=destination_path)

    def _populate_people_dim_csv(self, data: pd.DataFrame, destination_path: str):
        victims_agg_dim_filename = 'traffic_victims_agg_dim.csv'
        destination_path = os.path.join(destination_path, victims_agg_dim_filename)
        accident_traffic_columns = {'CRASH_RECORD_ID': 'IdIncident',
                                    'NUM_PASSENGER_VICTIMS': 'numPassengerVictims',
                                    'NUM_DRIVER_VICTIMS': 'numDriverVictims',
                                    'NUM_PEDESTRIAN_VICTIMS': 'numPedestrianVictims',
                                    'NUM_MALES': 'numMales',
                                    'NUM_FEMALES': 'numFemales',
                                    'NUM_CHILDREN': 'numChildren',
                                    'NUM_ADULTS': 'numAdults',
                                    'NUM_SENIORS': 'numSeniors'
                                    }
        # passenger type aggregation

        passenger_type_aggregation_df = data.filter(items=['CRASH_RECORD_ID', 'PERSON_TYPE']).loc[data['PERSON_TYPE'].isin(['PEDESTRIAN', 'DRIVER', 'PASSENGER',])]
        passenger_type_aggregation_df = passenger_type_aggregation_df.groupby(by=['CRASH_RECORD_ID', 'PERSON_TYPE']).size().unstack(fill_value=0)
        passenger_type_aggregation_df = passenger_type_aggregation_df.rename(columns={'PEDESTRIAN': 'NUM_PEDESTRIAN_VICTIMS',
                                                                                      'DRIVER': 'NUM_DRIVER_VICTIMS',
                                                                                      'PASSENGER': 'NUM_PASSENGER_VICTIMS'}).reset_index()

        # gender aggregation
        data_grouped_person_type_df = data.filter(items=['CRASH_RECORD_ID', 'SEX']).loc[data['SEX'].isin(['M', 'F',])]
        data_grouped_person_type_df = data_grouped_person_type_df.groupby(by=['CRASH_RECORD_ID', 'SEX']).size().unstack(fill_value=0)
        data_grouped_person_type_df = data_grouped_person_type_df.rename(columns={'M': 'NUM_MALES', 'F': 'NUM_FEMALES'}).reset_index()

        bins = [0, 18, 60, float('inf')]
        labels = ['CHILDREN', 'ADULTS', 'SENIORS']

        data_grouped_by_age_group_df = data.filter(items=['CRASH_RECORD_ID', 'AGE'])
        data_grouped_by_age_group_df['AGE_GROUP'] = pd.cut(data['AGE'], bins=bins, labels=labels, right=False)
        data_grouped_by_age_group_df = data_grouped_by_age_group_df.groupby(['CRASH_RECORD_ID', 'AGE_GROUP']).size().unstack(fill_value=0)
        data_grouped_by_age_group_df = data_grouped_by_age_group_df.rename(columns=lambda x: f'NUM_{x.upper()}').reset_index()

        df_merged = reduce(lambda left, right: pd.merge(left, right, on=['CRASH_RECORD_ID'],
                                                        how='outer'),
                           [passenger_type_aggregation_df,
                                     data_grouped_person_type_df,
                                     data_grouped_by_age_group_df]).fillna(0)
        for column in list(accident_traffic_columns):
            if column != 'CRASH_RECORD_ID':
                df_merged[column] = df_merged[column].astype('Int64')
        self._populate_to_csv(data=df_merged, destination_path=destination_path, columns_mapping=accident_traffic_columns)
