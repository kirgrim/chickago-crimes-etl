from .base import TrafficCrashesLoader
from .orm.models import TrafficAccident, TrafficAccidentVictimsInChicago


class TrafficCrashesDataLoader(TrafficCrashesLoader):

    def __init__(self):
        self.injuries_per_accident = {}

    def create_single_item(self, row_data: dict):
        return TrafficAccident(
            id=row_data['IdIncident'],
            weather_condition=row_data['WeatherCondition'],
            lighting_condition=row_data['LightingCondition'],
            road_surface_condition=row_data['RoadwaySurfaceCond'],
            damage=row_data['Damage'],
            posted_speed_limit=row_data['PostedSpeedLimit'],
            crash_type=row_data['CrashType'],
            cause=row_data['Cause'],
        )

    @property
    def fact_column_id(self) -> str:
        return "id_traffic_accident"

    @property
    def csv_source_file(self) -> str:
        return "accident_traffic_dim.csv"

    def _update_fact_table_with_inserted_items(self, session, item_accident_ids, inserted_ids):
        insert_records = [TrafficAccidentVictimsInChicago(id_traffic_accident=item_accident_ids[i],
                                                          injuries_total=self.injuries_per_accident[item_accident_ids[i]]['total'],
                                                          injuries_fatal=self.injuries_per_accident[item_accident_ids[i]]['fatal'])
                          for i in range(len(item_accident_ids))]
        session.bulk_save_objects(insert_records)

    def _store_extras(self, row: dict):
        self.injuries_per_accident[row['IdIncident']] = {'total': row['InjuriesTotal'], 'fatal': row['InjuriesFatal']}
