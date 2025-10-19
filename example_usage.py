"""
Example usage of the Weather Data Pipeline
This script demonstrates how to use the pipeline components
"""
import os
from weather_pipeline import WeatherDataPipeline
from config import Config

def example_single_run():
    """Example of running a single data collection"""
    print("=== Single Data Collection Example ===")
    
    # Initialize pipeline
    pipeline = WeatherDataPipeline()
    
    # Run single collection for specific cities
    cities = ["London", "New York", "Tokyo"]
    result = pipeline.run_single_collection(cities)
    
    print(f"Collection Result: {result}")
    
    # Clean up
    pipeline.cleanup()

def example_check_status():
    """Example of checking pipeline status"""
    print("=== Pipeline Status Check Example ===")
    
    pipeline = WeatherDataPipeline()
    
    try:
        status = pipeline.get_pipeline_status()
        print("Pipeline Status:")
        for key, value in status.items():
            print(f"  {key}: {value}")
    finally:
        pipeline.cleanup()

def example_continuous_collection():
    """Example of running continuous collection (limited time)"""
    print("=== Continuous Collection Example (will run for 5 minutes) ===")
    
    import time
    
    pipeline = WeatherDataPipeline()
    
    try:
        import schedule
        
        # Schedule collection every 5 minutes for demo
        schedule.every(5).minutes.do(pipeline.run_single_collection)
        
        # Run initial collection
        pipeline.run_single_collection()
        
        # Run for 5 minutes then stop
        start_time = time.time()
        while time.time() - start_time < 300:  # 5 minutes
            schedule.run_pending()
            time.sleep(60)  # Check every minute
            
    except KeyboardInterrupt:
        print("Stopped by user")
    finally:
        pipeline.cleanup()

def main():
    """Main example runner"""
    print("Weather Data Pipeline - Example Usage")
    print("====================================")
    
    # Check if API key is configured
    if not Config.OPENWEATHER_API_KEY:
        print("ERROR: Please set your OpenWeatherMap API key in the .env file")
        print("Example: OPENWEATHER_API_KEY=your_api_key_here")
        return
    
    print("\nAvailable examples:")
    print("1. Single data collection")
    print("2. Check pipeline status")
    print("3. Run continuous collection (5 minutes)")
    
    choice = input("\nEnter your choice (1-3): ").strip()
    
    if choice == "1":
        example_single_run()
    elif choice == "2":
        example_check_status()
    elif choice == "3":
        example_continuous_collection()
    else:
        print("Invalid choice")

if __name__ == "__main__":
    main()
