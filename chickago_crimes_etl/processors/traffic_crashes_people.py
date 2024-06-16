import os
import pandas as pd

from .base import TrafficCrashesCSVProcessor


class TrafficCrashesPeopleCSVProcessor(TrafficCrashesCSVProcessor):

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
        for column in list(accident_traffic_columns):
            if column != 'CRASH_RECORD_ID':
                data[column] = data[column].astype('Int64')
        self._populate_to_csv(data=data, destination_path=destination_path, columns_mapping=accident_traffic_columns)
