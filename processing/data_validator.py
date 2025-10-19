"""
Data validation and transformation utilities
"""
from datetime import datetime
from typing import Dict, List, Optional
import pytz
from loguru import logger

class WeatherDataValidator:
    """Validates and cleans weather data"""
    
    @staticmethod
    def validate_weather_data(data: Dict) -> bool:
        """
        Validate weather data structure and values
        
        Args:
            data (Dict): Weather data dictionary
            
        Returns:
            bool: True if data is valid, False otherwise
        """
        required_fields = [
            'city', 'country', 'timestamp', 'temperature', 
            'temperature_celsius', 'humidity', 'pressure', 
            'description', 'weather_main'
        ]
        
        # Check required fields
        for field in required_fields:
            if field not in data or data[field] is None:
                logger.warning(f"Missing required field: {field}")
                return False
        
        # Validate data types and ranges
        try:
            # Temperature validation (in Celsius)
            temp_c = float(data['temperature_celsius'])
            if temp_c < -100 or temp_c > 60:  # Reasonable temperature range
                logger.warning(f"Temperature out of range: {temp_c}°C")
                return False
            
            # Humidity validation (0-100%)
            humidity = float(data['humidity'])
            if humidity < 0 or humidity > 100:
                logger.warning(f"Humidity out of range: {humidity}%")
                return False
            
            # Pressure validation (800-1200 hPa)
            pressure = float(data['pressure'])
            if pressure < 800 or pressure > 1200:
                logger.warning(f"Pressure out of range: {pressure} hPa")
                return False
            
            # Wind speed validation (if present)
            if data.get('wind_speed') is not None:
                wind_speed = float(data['wind_speed'])
                if wind_speed < 0 or wind_speed > 200:  # Very high wind speeds
                    logger.warning(f"Wind speed out of range: {wind_speed} m/s")
                    return False
            
            # Cloudiness validation (0-100%)
            cloudiness = float(data.get('cloudiness', 0))
            if cloudiness < 0 or cloudiness > 100:
                logger.warning(f"Cloudiness out of range: {cloudiness}%")
                return False
                
        except (ValueError, TypeError) as e:
            logger.warning(f"Data type validation error: {e}")
            return False
        
        return True
    
    @staticmethod
    def clean_weather_data(data: Dict) -> Dict:
        """
        Clean and standardize weather data
        
        Args:
            data (Dict): Raw weather data
            
        Returns:
            Dict: Cleaned weather data
        """
        cleaned_data = data.copy()
        
        # Convert timestamp to datetime if it's a Unix timestamp
        if isinstance(data.get('timestamp'), (int, float)):
            cleaned_data['timestamp'] = datetime.fromtimestamp(
                data['timestamp'], tz=pytz.UTC
            )
        
        # Ensure numeric fields are properly typed
        numeric_fields = [
            'temperature', 'temperature_celsius', 'feels_like', 
            'humidity', 'pressure', 'visibility', 'wind_speed', 
            'wind_direction', 'cloudiness'
        ]
        
        for field in numeric_fields:
            if field in cleaned_data and cleaned_data[field] is not None:
                try:
                    cleaned_data[field] = float(cleaned_data[field])
                except (ValueError, TypeError):
                    cleaned_data[field] = None
        
        # Clean string fields
        string_fields = ['city', 'country', 'description', 'weather_main']
        for field in string_fields:
            if field in cleaned_data:
                cleaned_data[field] = str(cleaned_data[field]).strip()
        
        return cleaned_data
    
    @staticmethod
    def filter_valid_records(weather_records: List[Dict]) -> List[Dict]:
        """
        Filter out invalid weather records
        
        Args:
            weather_records (List[Dict]): List of weather data records
            
        Returns:
            List[Dict]: List of valid weather data records
        """
        valid_records = []
        
        for record in weather_records:
            if WeatherDataValidator.validate_weather_data(record):
                cleaned_record = WeatherDataValidator.clean_weather_data(record)
                valid_records.append(cleaned_record)
            else:
                logger.warning(f"Dropping invalid record for city: {record.get('city', 'Unknown')}")
        
        logger.info(f"Filtered {len(valid_records)}/{len(weather_records)} valid records")
        return valid_records
