"""
Main Weather Data Pipeline - Orchestrates data collection, processing, and storage
"""
import sys
import time
from typing import List, Dict
from loguru import logger
from datetime import datetime

# Import our modules
from config import Config
from api.weather_client import WeatherAPIClient
from processing.data_validator import WeatherDataValidator
from database.models import DatabaseManager
from storage.data_storage import WeatherDataStorage

class WeatherDataPipeline:
    """Main pipeline class that orchestrates the entire weather data flow"""
    
    def __init__(self):
        # Initialize components
        self.api_client = WeatherAPIClient()
        self.db_manager = DatabaseManager()
        self.storage = WeatherDataStorage(self.db_manager)
        
        # Setup logging
        logger.add(
            Config.LOG_FILE,
            rotation="1 day",
            retention="30 days",
            level=Config.LOG_LEVEL,
            format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}"
        )
        
        logger.info("Weather Data Pipeline initialized")
    
    def run_single_collection(self, cities: List[str] = None) -> Dict:
        """
        Run a single data collection cycle
        
        Args:
            cities (List[str], optional): List of cities to collect data for
            
        Returns:
            Dict: Collection results summary
        """
        if cities is None:
            cities = Config.CITIES
        
        logger.info(f"Starting weather data collection for cities: {cities}")
        
        try:
            # Test API connection first
            if not self.api_client.test_connection():
                raise ConnectionError("Failed to connect to weather API")
            
            # Fetch weather data
            raw_weather_data = self.api_client.get_multiple_cities_weather(cities)
            
            if not raw_weather_data:
                logger.warning("No weather data received from API")
                return {"status": "failed", "collection_time": datetime.utcnow()}
            
            # Validate and clean data
            valid_weather_data = WeatherDataValidator.filter_valid_records(raw_weather_data)
            
            if not valid_weather_data:
                logger.error("No valid weather data after validation")
                return {"status": "failed", "collection_time": datetime.utcnow()}
            
            # Store data
            stored_count = self.storage.store_weather_data(valid_weather_data)
            
            result = {
                "status": "success",
                "collection_time": datetime.utcnow(),
                "cities_requested": len(cities),
                "cities_received": len(raw_weather_data),
                "records_validated": len(valid_weather_data),
                "records_stored": stored_count
            }
            
            logger.info(f"Collection completed successfully: {result}")
            return result
            
        except Exception as e:
            logger.error(f"Error during data collection: {e}")
            return {
                "status": "error",
                "error": str(e),
                "collection_time": datetime.utcnow()
            }
    
    def get_pipeline_status(self) -> Dict:
        """
        Get current pipeline status and statistics
        
        Returns:
            Dict: Pipeline status information
        """
        try:
            # Get latest weather data
            latest_data = self.storage.get_latest_weather()
            
            # Get weather statistics
            stats = self.storage.get_weather_stats(days=7)
            
            # Test API connection
            api_status = self.api_client.test_connection()
            
            status = {
                "pipeline_health": "healthy" if api_status else "unhealthy",
                "api_connection": api_status,
                "last_update": latest_data[0].timestamp.isoformat() if latest_data else None,
                "total_cities_monitored": len(set(record.city for record in latest_data)),
                "recent_statistics": stats,
                "database_connected": True  # If we got here, DB is working
            }
            
            return status
            
        except Exception as e:
            logger.error(f"Error getting pipeline status: {e}")
            return {
                "pipeline_health": "error",
                "error": str(e)
            }
    
    def run_continuous(self, interval_minutes: int = None):
        """
        Run the pipeline continuously with scheduled collections
        
        Args:
            interval_minutes (int, optional): Collection interval in minutes
        """
        if interval_minutes is None:
            interval_minutes = Config.UPDATE_INTERVAL_MINUTES
        
        logger.info(f"Starting continuous weather data collection every {interval_minutes} minutes")
        
        try:
            import schedule
            
            # Schedule the collection job
            schedule.every(interval_minutes).minutes.do(self.run_single_collection)
            
            # Run initial collection
            logger.info("Running initial data collection...")
            self.run_single_collection()
            
            # Keep the pipeline running
            while True:
                schedule.run_pending()
                time.sleep(60)  # Check every minute
                
        except KeyboardInterrupt:
            logger.info("Pipeline stopped by user")
        except Exception as e:
            logger.error(f"Error in continuous pipeline: {e}")
        finally:
            self.cleanup()
    
    def cleanup(self):
        """Cleanup resources"""
        logger.info("Cleaning up pipeline resources...")
        self.db_manager.close()

def main():
    """Main entry point for the weather pipeline"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Weather Data Pipeline")
    parser.add_argument(
        "--mode", 
        choices=["single", "continuous"], 
        default="single",
        help="Run mode: single collection or continuous monitoring"
    )
    parser.add_argument(
        "--cities", 
        nargs="+",
        help="Cities to monitor (default: from config)"
    )
    parser.add_argument(
        "--interval", 
        type=int,
        help="Collection interval in minutes for continuous mode"
    )
    parser.add_argument(
        "--status", 
        action="store_true",
        help="Show pipeline status and exit"
    )
    
    args = parser.parse_args()
    
    # Initialize pipeline
    pipeline = WeatherDataPipeline()
    
    try:
        if args.status:
            # Show status and exit
            status = pipeline.get_pipeline_status()
            print("Pipeline Status:")
            for key, value in status.items():
                print(f"  {key}: {value}")
            return
        
        if args.mode == "single":
            # Single collection run
            result = pipeline.run_single_collection(args.cities)
            print(f"Collection result: {result}")
            
            if result.get("status") == "success":
                print(f"Successfully collected weather data for {result['records_stored']} cities")
            else:
                print(f"Collection failed: {result.get('error', 'Unknown error')}")
                sys.exit(1)
                
        elif args.mode == "continuous":
            # Continuous monitoring
            pipeline.run_continuous(args.interval)
            
    except KeyboardInterrupt:
        logger.info("Pipeline interrupted by user")
    except Exception as e:
        logger.error(f"Pipeline error: {e}")
        sys.exit(1)
    finally:
        pipeline.cleanup()

if __name__ == "__main__":
    main()
