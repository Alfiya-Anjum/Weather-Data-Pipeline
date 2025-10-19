"""
Weather API client for fetching data from OpenWeatherMap
"""
import requests
import time
from typing import Dict, List, Optional
from loguru import logger
from config import Config

class WeatherAPIClient:
    def __init__(self, api_key: str = None):
        self.api_key = api_key or Config.OPENWEATHER_API_KEY
        self.base_url = Config.OPENWEATHER_BASE_URL
        self.session = requests.Session()
        
        if not self.api_key:
            raise ValueError("OpenWeatherMap API key is required")
    
    def get_current_weather(self, city: str) -> Optional[Dict]:
        """
        Fetch current weather data for a specific city
        
        Args:
            city (str): City name
            
        Returns:
            Dict: Weather data or None if request fails
        """
        url = f"{self.base_url}/weather"
        params = {
            'q': city,
            'appid': self.api_key,
            'units': 'metric'  # Get temperature in Celsius
        }
        
        try:
            logger.info(f"Fetching weather data for {city}")
            response = self.session.get(url, params=params, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            
            # Transform the data to our standard format
            return self._transform_weather_data(data)
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching weather data for {city}: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error fetching weather data for {city}: {e}")
            return None
    
    def get_multiple_cities_weather(self, cities: List[str]) -> List[Dict]:
        """
        Fetch weather data for multiple cities with rate limiting
        
        Args:
            cities (List[str]): List of city names
            
        Returns:
            List[Dict]: List of weather data dictionaries
        """
        weather_data = []
        
        for city in cities:
            data = self.get_current_weather(city)
            if data:
                weather_data.append(data)
            
            # Rate limiting - sleep for 1 second between requests
            # (OpenWeatherMap allows 60 requests/minute for free tier)
            time.sleep(1)
        
        logger.info(f"Successfully fetched weather data for {len(weather_data)}/{len(cities)} cities")
        return weather_data
    
    def _transform_weather_data(self, raw_data: Dict) -> Dict:
        """
        Transform raw API response to standardized format
        
        Args:
            raw_data (Dict): Raw API response
            
        Returns:
            Dict: Transformed weather data
        """
        try:
            return {
                'city': raw_data['name'],
                'country': raw_data['sys']['country'],
                'timestamp': raw_data['dt'],
                'temperature': raw_data['main']['temp'] + 273.15,  # Convert to Kelvin for storage
                'temperature_celsius': raw_data['main']['temp'],
                'feels_like': raw_data['main']['feels_like'] + 273.15,
                'humidity': raw_data['main']['humidity'],
                'pressure': raw_data['main']['pressure'],
                'visibility': raw_data.get('visibility'),
                'wind_speed': raw_data.get('wind', {}).get('speed'),
                'wind_direction': raw_data.get('wind', {}).get('deg'),
                'cloudiness': raw_data['clouds']['all'],
                'description': raw_data['weather'][0]['description'],
                'weather_main': raw_data['weather'][0]['main']
            }
        except KeyError as e:
            logger.error(f"Missing key in weather data: {e}")
            raise ValueError(f"Invalid weather data format: missing {e}")
    
    def test_connection(self) -> bool:
        """
        Test API connection by fetching weather for a test city
        
        Returns:
            bool: True if connection successful, False otherwise
        """
        try:
            test_data = self.get_current_weather("London")
            return test_data is not None
        except Exception as e:
            logger.error(f"API connection test failed: {e}")
            return False
