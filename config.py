"""
Configuration settings for the Weather Data Pipeline
"""
import os
from typing import List
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class Config:
    # API Configuration
    OPENWEATHER_API_KEY = os.getenv('OPENWEATHER_API_KEY', '')
    OPENWEATHER_BASE_URL = os.getenv('OPENWEATHER_BASE_URL', 'https://api.openweathermap.org/data/2.5')
    
    # Database Configuration
    DATABASE_PATH = os.getenv('DATABASE_PATH', 'weather_data.db')
    
    # Pipeline Configuration
    DEFAULT_CITIES = ["London", "New York", "Tokyo", "Paris", "Sydney"]
    CITIES = os.getenv('CITIES', ','.join(DEFAULT_CITIES)).split(',')
    UPDATE_INTERVAL_MINUTES = int(os.getenv('UPDATE_INTERVAL_MINUTES', '30'))
    
    # Logging Configuration
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
    LOG_FILE = os.getenv('LOG_FILE', 'weather_pipeline.log')

# Validate required configuration
if not Config.OPENWEATHER_API_KEY:
    raise ValueError("OPENWEATHER_API_KEY is required. Please set it in your .env file.")
