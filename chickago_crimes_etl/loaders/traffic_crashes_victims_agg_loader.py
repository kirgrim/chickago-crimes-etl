import pandas as pd

from .base import TrafficCrashesLoader
from .orm.models import TrafficAccidentVictimsAgg


class TrafficCrashesVictimsAggLoader(TrafficCrashesLoader):

    @property
    def target_table(self):
        return TrafficAccidentVictimsAgg

    @property
    def fact_column_id(self) -> str:
        return 'idTrafficAccidentVictimsAgg'

    @property
    def csv_source_file(self) -> str:
        return 'traffic_victims_agg_dim.csv'

    def clean_data(self, data):
        for column in (
            'numPassengerVictims',
            'numDriverVictims',
            'numPedestrianVictims',
            'numMales',
            'numFemales',
            'numChildren',
            'numAdults',
            'numSeniors'
        ):
            try:
                data = data[pd.to_numeric(data[column], errors='coerce').notnull()]
                data[column] = data[column].astype(int)
            except ValueError:
                print(column)
        return data
