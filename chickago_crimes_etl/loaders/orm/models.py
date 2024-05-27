from time import time

from sqlalchemy import create_engine, Column, Integer, String, ForeignKey, Numeric, BigInteger
from sqlalchemy.orm import sessionmaker, declarative_base

from .auth import DB_USERNAME, DB_PASSWORD, DB_SERVER_URL, SQL_DRIVER, SQL_DATABASE


Base = declarative_base()


class TrafficAccidentLocation(Base):
    __tablename__ = 'trafficAccidentLocation'
    id = Column(BigInteger, primary_key=True, default=0, name='idLocation')
    latitude = Column(Numeric(10, 3, asdecimal=False), name='latitude')
    longitude = Column(Numeric(10,3, asdecimal=False), name='longitude')
    crash_location = Column(String(128), name='crashLocation')
    street_no = Column(Integer, name='streetNo')
    street_name = Column(String(128), name='streetName')


class TrafficAccidentVictimsAgg(Base):
    __tablename__ = 'trafficAccidentVictimsAgg'
    id = Column(Integer, primary_key=True, autoincrement=True, name='idVictimsAgg')
    num_passenger_victims = Column(Integer, name='numPassengerVictims')
    num_driver_victims = Column(Integer, name='numDriverVictims')
    num_pedestrian_victims = Column(String(128), name='numPedestrianVictims')
    num_males = Column(Integer, name='numMales')
    num_females = Column(Integer, name='numFemales')
    num_children = Column(Integer, name='numChildren')
    num_adults = Column(Integer, name='numAdults')
    num_seniors = Column(Integer, name='numSeniors')


class TrafficAccident(Base):
    __tablename__ = 'trafficAccident'
    id = Column(String(128), primary_key=True, name='idAccidentTraffic')
    weather_condition = Column(String(64), name='weatherCondition')
    lighting_condition = Column(String(64), name='lightingCondition')
    road_surface_condition = Column(String(64), name='roadSurfaceCondition')
    damage = Column(String(64), name='damage')
    posted_speed_limit = Column(Integer, name='postedSpeedLimit')
    crash_type = Column(String(128), name='crashType')
    cause = Column(String(128), name='cause')


class TrafficAccidentTime(Base):
    __tablename__ = 'trafficAccidentTime'
    id = Column(Integer, primary_key=True, autoincrement=True, name='timeId')
    date = Column(String(64), name='date')
    year = Column(Integer, name='year')
    month = Column(Integer, name='month')
    month_name = Column(String(32), name='monthName')
    week_day = Column(Integer, name='weekDay')
    week_day_name = Column(String(16), name='weekDayName')
    day = Column(Integer, name='day')
    hour = Column(Integer, name='hour')
    minute = Column(Integer, name='minute')


class TrafficAccidentVehicle(Base):
    __tablename__ = 'trafficAccidentVehicle'
    id = Column(Integer, primary_key=True, autoincrement=True, name='idVehicle')
    vehicle_make = Column(String(64), name='vehicleMake')
    vehicle_model = Column(String(64), name='vehicleModel')
    vehicle_year = Column(Integer, name='vehicleYear')
    vehicle_type = Column(String(64), name='vehicleType')
    vehicle_use = Column(String(64), name='vehicleUse')
    maneuver = Column(String(128), name='maneuver')


class TrafficAccidentVictimsInChicago(Base):
    __tablename__ = 'trafficAccidentVictimsInChicago'
    id_traffic_accident = Column(String(128), ForeignKey('trafficAccident.idAccidentTraffic'), primary_key=True, name='idTrafficAccident', autoincrement=False)
    id_traffic_accident_time = Column(Integer, ForeignKey('trafficAccidentTime.timeId'), name='idTrafficAccidentTime', nullable=True, default=None, autoincrement=False)
    id_traffic_accident_police_notified = Column(Integer, ForeignKey('trafficAccidentTime.timeId'), name='idTrafficAccidentPoliceNotified', nullable=True, default=None, autoincrement=False)
    id_traffic_accident_victims_agg = Column(Integer, ForeignKey('trafficAccidentVictimsAgg.idVictimsAgg'), name='idTrafficAccidentVictimsAgg', nullable=True, default=None, autoincrement=False)
    id_traffic_accident_location = Column(Integer, ForeignKey('trafficAccidentLocation.idLocation'), name='idTrafficAccidentLocation', nullable=True, default=None, autoincrement=False)
    id_traffic_accident_vehicle = Column(Integer, ForeignKey('trafficAccidentVehicle.idVehicle'), name='idTrafficAccidentVehicle', nullable=True, default=None, autoincrement=False)
    time_between_crash_and_police_notification = Column(Integer, name='timeBetweenCrashAndPoliceNotification', nullable=True, default=None)
    injuries_total = Column(Integer, name='injuriesTotal', nullable=True, default=None)
    injuries_fatal = Column(Integer, name='injuriesFatal', nullable=True, default=None)


connection_string = f'mssql+pyodbc://{DB_USERNAME}:{DB_PASSWORD}@{DB_SERVER_URL}/{SQL_DATABASE}?driver={SQL_DRIVER}'
engine = create_engine(url=connection_string, echo=True)

Base.metadata.create_all(engine)

Session = sessionmaker(bind=engine)
