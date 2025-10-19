"""
Data storage utilities for weather data
"""
from typing import List, Dict, Optional
from datetime import datetime, timedelta
import pytz
from sqlalchemy.orm import Session
from loguru import logger
from database.models import WeatherData, DatabaseManager

class WeatherDataStorage:
    """Handles storage and retrieval of weather data"""
    
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
    
    def store_weather_data(self, weather_records: List[Dict]) -> int:
        """
        Store weather data records in the database
        
        Args:
            weather_records (List[Dict]): List of weather data dictionaries
            
        Returns:
            int: Number of records stored
        """
        if not weather_records:
            logger.warning("No weather records to store")
            return 0
        
        session = self.db_manager.get_session()
        stored_count = 0
        
        try:
            for record in weather_records:
                # Convert timestamp if it's a Unix timestamp
                timestamp = record.get('timestamp')
                if isinstance(timestamp, (int, float)):
                    # Convert Unix timestamp to datetime
                    record['timestamp'] = datetime.fromtimestamp(timestamp, tz=pytz.UTC)
                elif not isinstance(timestamp, datetime):
                    # Default to current time if timestamp is invalid
                    record['timestamp'] = datetime.utcnow()
                
                # Create WeatherData object
                weather_obj = WeatherData(
                    city=record['city'],
                    country=record['country'],
                    timestamp=record['timestamp'],
                    temperature=record['temperature'],
                    temperature_celsius=record['temperature_celsius'],
                    feels_like=record.get('feels_like'),
                    humidity=record['humidity'],
                    pressure=record['pressure'],
                    visibility=record.get('visibility'),
                    wind_speed=record.get('wind_speed'),
                    wind_direction=record.get('wind_direction'),
                    cloudiness=record.get('cloudiness', 0),
                    description=record['description'],
                    weather_main=record['weather_main']
                )
                
                session.add(weather_obj)
                stored_count += 1
            
            session.commit()
            logger.info(f"Successfully stored {stored_count} weather records")
            
        except Exception as e:
            session.rollback()
            logger.error(f"Error storing weather data: {e}")
            raise
        finally:
            session.close()
        
        return stored_count
    
    def get_latest_weather(self, city: str = None) -> List[WeatherData]:
        """
        Get the latest weather data for a city or all cities
        
        Args:
            city (str, optional): City name. If None, returns data for all cities.
            
        Returns:
            List[WeatherData]: Latest weather records
        """
        session = self.db_manager.get_session()
        
        try:
            query = session.query(WeatherData)
            
            if city:
                query = query.filter(WeatherData.city.ilike(f"%{city}%"))
            
            # Get the latest record for each city
            latest_records = query.order_by(
                WeatherData.city, 
                WeatherData.timestamp.desc()
            ).all()
            
            # Filter to get only the most recent record per city
            seen_cities = set()
            result = []
            for record in latest_records:
                if record.city not in seen_cities:
                    result.append(record)
                    seen_cities.add(record.city)
            
            return result
            
        except Exception as e:
            logger.error(f"Error retrieving latest weather data: {e}")
            return []
        finally:
            session.close()
    
    def get_weather_history(self, city: str, days: int = 7) -> List[WeatherData]:
        """
        Get weather history for a specific city
        
        Args:
            city (str): City name
            days (int): Number of days to retrieve
            
        Returns:
            List[WeatherData]: Historical weather records
        """
        session = self.db_manager.get_session()
        
        try:
            start_date = datetime.utcnow() - timedelta(days=days)
            
            records = session.query(WeatherData).filter(
                WeatherData.city.ilike(f"%{city}%"),
                WeatherData.timestamp >= start_date
            ).order_by(WeatherData.timestamp.desc()).all()
            
            return records
            
        except Exception as e:
            logger.error(f"Error retrieving weather history for {city}: {e}")
            return []
        finally:
            session.close()
    
    def get_weather_stats(self, city: str = None, days: int = 7) -> Dict:
        """
        Get weather statistics for a city or all cities
        
        Args:
            city (str, optional): City name
            days (int): Number of days to analyze
            
        Returns:
            Dict: Weather statistics
        """
        session = self.db_manager.get_session()
        
        try:
            start_date = datetime.utcnow() - timedelta(days=days)
            
            query = session.query(WeatherData).filter(
                WeatherData.timestamp >= start_date
            )
            
            if city:
                query = query.filter(WeatherData.city.ilike(f"%{city}%"))
            
            records = query.all()
            
            if not records:
                return {}
            
            # Calculate statistics
            temps = [r.temperature_celsius for r in records]
            humidity = [r.humidity for r in records]
            pressure = [r.pressure for r in records]
            
            stats = {
                'total_records': len(records),
                'avg_temperature': sum(temps) / len(temps),
                'min_temperature': min(temps),
                'max_temperature': max(temps),
                'avg_humidity': sum(humidity) / len(humidity),
                'avg_pressure': sum(pressure) / len(pressure),
                'cities_covered': len(set(r.city for r in records)),
                'date_range': f"{start_date.date()} to {datetime.utcnow().date()}"
            }
            
            return stats
            
        except Exception as e:
            logger.error(f"Error calculating weather stats: {e}")
            return {}
        finally:
            session.close()
