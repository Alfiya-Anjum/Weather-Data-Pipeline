"""
Main Weather Data Pipeline - Orchestrates data collection, validation, storage, and cloud upload.
"""

import sys
import time
from datetime import datetime
from typing import List, Dict
from loguru import logger

# Import our internal modules
from config import Config
from api.weather_client import WeatherAPIClient
from processing.data_validator import WeatherDataValidator
from database.models import DatabaseManager
from storage.data_storage import WeatherDataStorage
from storage.bigquery_loader import BigQueryLoader


class WeatherDataPipeline:
    """Main pipeline class that orchestrates the entire weather data flow"""

    def __init__(self):
        # Initialize components
        self.api_client = WeatherAPIClient()
        self.db_manager = DatabaseManager()
        self.storage = WeatherDataStorage(self.db_manager)

        # Add BigQuery loader
        self.bq_loader = BigQueryLoader(
            project_id="weather-etl-project-475620",     # ⬅️ Replace this with your actual GCP project ID
            dataset_id="weather_data",
            table_id="weather_records"
        )

        # Setup logging
        logger.add(
            Config.LOG_FILE,
            rotation="1 day",
            retention="30 days",
            level=Config.LOG_LEVEL,
            format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}"
        )

        logger.info("✅ Weather Data Pipeline initialized successfully")

    # -----------------------------------------------------
    # 1️⃣ SINGLE COLLECTION MODE
    # -----------------------------------------------------
    def run_single_collection(self, cities: List[str] = None) -> Dict:
        """Run a single data collection cycle"""
        if cities is None:
            cities = Config.CITIES

        logger.info(f"🌦 Starting weather data collection for: {cities}")

        try:
            # Test API connection
            if not self.api_client.test_connection():
                raise ConnectionError("Failed to connect to the weather API")

            # Step 1: Extract
            raw_weather_data = self.api_client.get_multiple_cities_weather(cities)
            if not raw_weather_data:
                logger.warning("⚠️ No weather data received from API.")
                return {"status": "failed", "collection_time": datetime.utcnow()}

            # Step 2: Transform / Validate
            valid_weather_data = WeatherDataValidator.filter_valid_records(raw_weather_data)
            if not valid_weather_data:
                logger.error("❌ No valid weather data after validation.")
                return {"status": "failed", "collection_time": datetime.utcnow()}

            # Step 3: Load - Local storage
            stored_count = self.storage.store_weather_data(valid_weather_data)

            # Step 4: Load - Cloud (BigQuery)
            uploaded_count = self.bq_loader.upload_records(valid_weather_data)

            # Step 5: Result summary
            result = {
                "status": "success",
                "collection_time": datetime.utcnow(),
                "cities_requested": len(cities),
                "records_validated": len(valid_weather_data),
                "records_stored": stored_count,
                "records_uploaded": uploaded_count
            }

            logger.info(f"✅ Collection completed successfully: {result}")
            return result

        except Exception as e:
            logger.exception(f"💥 Error during data collection: {e}")
            return {
                "status": "error",
                "error": str(e),
                "collection_time": datetime.utcnow()
            }

    # -----------------------------------------------------
    # 2️⃣ PIPELINE STATUS CHECK
    # -----------------------------------------------------
    def get_pipeline_status(self) -> Dict:
        """Get current pipeline status and statistics"""
        try:
            latest_data = self.storage.get_latest_weather()
            stats = self.storage.get_weather_stats(days=7)
            api_status = self.api_client.test_connection()

            status = {
                "pipeline_health": "healthy" if api_status else "unhealthy",
                "api_connection": api_status,
                "last_update": latest_data[0].timestamp.isoformat() if latest_data else None,
                "total_cities_monitored": len(set(record.city for record in latest_data)) if latest_data else 0,
                "recent_statistics": stats,
                "database_connected": True
            }

            logger.info(f"📊 Pipeline status: {status}")
            return status

        except Exception as e:
            logger.error(f"⚠️ Error getting pipeline status: {e}")
            return {
                "pipeline_health": "error",
                "error": str(e)
            }

    # -----------------------------------------------------
    # 3️⃣ CONTINUOUS MODE (Scheduled)
    # -----------------------------------------------------
    def run_continuous(self, interval_minutes: int = None):
        """Run the pipeline continuously at scheduled intervals"""
        import schedule

        if interval_minutes is None:
            interval_minutes = Config.UPDATE_INTERVAL_MINUTES

        logger.info(f"🕒 Starting continuous weather data collection every {interval_minutes} minutes")

        try:
            # Schedule the collection job
            schedule.every(interval_minutes).minutes.do(self.run_single_collection)

            # Run initial collection immediately
            logger.info("🚀 Running initial data collection...")
            self.run_single_collection()

            # Keep the pipeline running
            while True:
                schedule.run_pending()
                time.sleep(60)

        except KeyboardInterrupt:
            logger.info("🛑 Pipeline stopped manually by user")
        except Exception as e:
            logger.exception(f"💥 Error in continuous pipeline: {e}")
        finally:
            self.cleanup()

    # -----------------------------------------------------
    # 4️⃣ CLEANUP
    # -----------------------------------------------------
    def cleanup(self):
        """Cleanup resources and close DB connections"""
        logger.info("🧹 Cleaning up pipeline resources...")
        self.db_manager.close()


# -----------------------------------------------------
# 5️⃣ MAIN ENTRY POINT
# -----------------------------------------------------
def main():
    """Main entry point for the weather pipeline"""
    import argparse

    parser = argparse.ArgumentParser(description="Weather Data ETL Pipeline")
    parser.add_argument(
        "--mode",
        choices=["single", "continuous"],
        default="single",
        help="Run mode: single collection or continuous monitoring"
    )
    parser.add_argument(
        "--cities",
        nargs="+",
        help="Cities to collect data for (default: from config)"
    )
    parser.add_argument(
        "--interval",
        type=int,
        help="Collection interval in minutes for continuous mode"
    )
    parser.add_argument(
        "--status",
        action="store_true",
        help="Show current pipeline status and exit"
    )

    args = parser.parse_args()
    pipeline = WeatherDataPipeline()

    try:
        if args.status:
            status = pipeline.get_pipeline_status()
            print("\n📊 Pipeline Status:")
            for k, v in status.items():
                print(f"  {k}: {v}")
            return

        if args.mode == "single":
            result = pipeline.run_single_collection(args.cities)
            print("\n✅ Collection result:")
            print(result)

        elif args.mode == "continuous":
            pipeline.run_continuous(args.interval)

    except KeyboardInterrupt:
        logger.info("🛑 Pipeline interrupted by user.")
    except Exception as e:
        logger.exception(f"💥 Pipeline encountered an error: {e}")
        sys.exit(1)
    finally:
        pipeline.cleanup()


if __name__ == "__main__":
    main()
