"""
Database models for weather data storage
"""
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, Index
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime
import pytz

Base = declarative_base()

class WeatherData(Base):
    __tablename__ = 'weather_data'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    city = Column(String(100), nullable=False)
    country = Column(String(100), nullable=False)
    timestamp = Column(DateTime, nullable=False, default=datetime.utcnow)
    temperature = Column(Float, nullable=False)  # in Kelvin
    temperature_celsius = Column(Float, nullable=False)  # converted to Celsius
    feels_like = Column(Float, nullable=False)
    humidity = Column(Float, nullable=False)  # percentage
    pressure = Column(Float, nullable=False)  # hPa
    visibility = Column(Float, nullable=True)  # meters
    wind_speed = Column(Float, nullable=True)  # m/s
    wind_direction = Column(Float, nullable=True)  # degrees
    cloudiness = Column(Float, nullable=False)  # percentage
    description = Column(String(200), nullable=False)
    weather_main = Column(String(100), nullable=False)
    
    # Create indexes for better query performance
    __table_args__ = (
        Index('idx_city_timestamp', 'city', 'timestamp'),
        Index('idx_timestamp', 'timestamp'),
    )

class DatabaseManager:
    def __init__(self, database_url: str = None):
        if database_url is None:
            from config import Config
            database_url = f"sqlite:///{Config.DATABASE_PATH}"
        
        self.engine = create_engine(database_url)
        Base.metadata.create_all(self.engine)
        SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)
        self.SessionLocal = SessionLocal
    
    def get_session(self):
        return self.SessionLocal()
    
    def close(self):
        self.engine.dispose()
