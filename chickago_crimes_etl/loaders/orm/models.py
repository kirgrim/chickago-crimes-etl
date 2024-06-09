from sqlalchemy import create_engine, Column, Integer, String, ForeignKey, Numeric, BigInteger
from sqlalchemy.engine import URL
from sqlalchemy.orm import declarative_base

from .auth import DB_USERNAME, DB_PASSWORD, DB_SERVER_URL, SQL_DRIVER, SQL_DATABASE


Base = declarative_base()


class TrafficAccidentLocation(Base):
    __tablename__ = 'trafficAccidentLocation'
    id = Column(BigInteger, primary_key=True, default=0, name='idTrafficAccidentLocation')
    latitude = Column(Numeric(10, 3, asdecimal=False), name='latitude')
    longitude = Column(Numeric(10,3, asdecimal=False), name='longitude')
    crashLocation = Column(String(128), name='crashLocation')
    streetNo = Column(Integer, name='streetNo')
    streetName = Column(String(128), name='streetName')


class TrafficAccidentVictimsAgg(Base):
    __tablename__ = 'trafficAccidentVictimsAgg'
    id = Column(BigInteger, primary_key=True, default=0, name='idTrafficAccidentVictimsAgg')
    numPassengerVictims = Column(Integer, name='numPassengerVictims')
    numDriverVictims = Column(Integer, name='numDriverVictims')
    numPedestrianVictims = Column(Integer, name='numPedestrianVictims')
    numMales = Column(Integer, name='numMales')
    numFemales = Column(Integer, name='numFemales')
    numChildren = Column(Integer, name='numChildren')
    numAdults = Column(Integer, name='numAdults')
    numSeniors = Column(Integer, name='numSeniors')


class TrafficAccident(Base):
    __tablename__ = 'trafficAccident'
    idAccidentTraffic = Column(String(128), primary_key=True, default="", name='idAccidentTraffic')
    weatherCondition = Column(String(64), name='weatherCondition')
    lightingCondition = Column(String(64), name='lightingCondition')
    roadSurfaceCondition = Column(String(64), name='roadSurfaceCondition')
    damage = Column(String(64), name='damage')
    postedSpeedLimit = Column(Integer, name='postedSpeedLimit')
    crashType = Column(String(128), name='crashType')
    cause = Column(String(128), name='cause')


class TrafficAccidentTime(Base):
    __tablename__ = 'trafficAccidentTime'
    id = Column(BigInteger, primary_key=True, default=0, name='timeId')
    date = Column(String(64), name='date')
    year = Column(Integer, name='year')
    month = Column(Integer, name='month')
    monthName = Column(String(32), name='monthName')
    weekDay = Column(Integer, name='weekDay')
    weekDayName = Column(String(16), name='weekDayName')
    day = Column(Integer, name='day')
    hour = Column(Integer, name='hour')
    minute = Column(Integer, name='minute')


class TrafficAccidentVehicle(Base):
    __tablename__ = 'trafficAccidentVehicle'
    id = Column(BigInteger, primary_key=True, default=0, name='idTrafficAccidentVehicle')
    vehicleMake = Column(String, name='vehicleMake')
    vehicleModel = Column(String, name='vehicleModel')
    vehicleYear = Column(BigInteger, name='vehicleYear')
    vehicleType = Column(String, name='vehicleType')
    vehicleUse = Column(String, name='vehicleUse')
    maneuver = Column(String, name='maneuver')


class TrafficAccidentVictimsInChicago(Base):
    __tablename__ = 'trafficAccidentVictimsInChicago'
    idAccidentTraffic = Column(String(128), ForeignKey('trafficAccident.idAccidentTraffic'), primary_key=True, name='idAccidentTraffic')
    idTrafficAccidentTime = Column(BigInteger, ForeignKey('trafficAccidentTime.timeId'), name='idTrafficAccidentTime', nullable=True, default=None, autoincrement=False)
    idTrafficAccidentPoliceNotified = Column(BigInteger, ForeignKey('trafficAccidentTime.timeId'), name='idTrafficAccidentPoliceNotified', nullable=True, default=None, autoincrement=False)
    idTrafficAccidentVictimsAgg = Column(BigInteger, ForeignKey('trafficAccidentVictimsAgg.idTrafficAccidentVictimsAgg'), name='idTrafficAccidentVictimsAgg', nullable=True, default=None, autoincrement=False)
    idTrafficAccidentLocation = Column(BigInteger, ForeignKey('trafficAccidentLocation.idTrafficAccidentLocation'), name='idTrafficAccidentLocation', nullable=True, default=None, autoincrement=False)
    idTrafficAccidentVehicle = Column(BigInteger, ForeignKey('trafficAccidentVehicle.idTrafficAccidentVehicle'), name='idTrafficAccidentVehicle', nullable=True, default=None, autoincrement=False)
    timeBetweenCrashAndPoliceNotification = Column(Integer, name='timeBetweenCrashAndPoliceNotification', nullable=True, default=None)
    injuriesTotal = Column(Integer, name='injuriesTotal', nullable=True, default=None)
    injuriesFatal = Column(Integer, name='injuriesFatal', nullable=True, default=None)


# connection_string = f'mssql+pyodbc://{DB_USERNAME}:{DB_PASSWORD}@{DB_SERVER_URL}/{SQL_DATABASE}?driver={SQL_DRIVER}&timeout=120'
DB_HOST, DB_PORT = DB_SERVER_URL.split(":")
ALCHEMY_URL = URL.create(drivername="mssql+pyodbc",
                         username=DB_USERNAME,
                         password=DB_PASSWORD,
                         host=DB_HOST,
                         port=DB_PORT,
                         database=SQL_DATABASE,
                         query={
                            "driver": "ODBC Driver 17 for SQL Server",
                            "timeout": "120",
                         })
engine = create_engine(url=ALCHEMY_URL, echo=True, fast_executemany=True)
