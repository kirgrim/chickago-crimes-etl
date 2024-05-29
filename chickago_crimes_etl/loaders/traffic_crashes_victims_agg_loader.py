from .base import TrafficCrashesLoader
from .orm.models import TrafficAccidentVictimsAgg


class TrafficCrashesVictimsAggLoader(TrafficCrashesLoader):

    def create_single_item(self, row_data: dict):
        return TrafficAccidentVictimsAgg(id=self._get_ts_id(),
                                         num_passenger_victims=row_data['NumPassengerVictims'],
                                         num_pedestrian_victims=row_data['NumPedestrianVictims'],
                                         num_driver_victims=row_data['NumDriverVictims'],
                                         num_males=row_data['NumMales'],
                                         num_females=row_data['NumFemales'],
                                         num_children=row_data['NumChildren'],
                                         num_adults=row_data['NumAdults'],
                                         num_seniors=row_data['NumSeniors'],)

    @property
    def fact_column_id(self) -> str:
        return 'id_traffic_accident_victims_agg'

    @property
    def csv_source_file(self) -> str:
        return 'traffic_victims_agg_dim.csv'
