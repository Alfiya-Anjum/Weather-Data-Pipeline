"""
Streamlit dashboard for weather data visualization and monitoring
"""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import sys
import os

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.models import DatabaseManager
from storage.data_storage import WeatherDataStorage
from config import Config

class WeatherDashboard:
    """Streamlit dashboard for weather data"""
    
    def __init__(self):
        self.db_manager = DatabaseManager()
        self.storage = WeatherDataStorage(self.db_manager)
    
    def run(self):
        """Run the Streamlit dashboard"""
        st.set_page_config(
            page_title="Weather Data Pipeline Dashboard",
            page_icon="🌤️",
            layout="wide"
        )
        
        st.title("🌤️ Weather Data Pipeline Dashboard")
        
        # Sidebar controls
        self._create_sidebar()
        
        # Main dashboard content
        self._display_overview()
        self._display_weather_data()
        self._display_analytics()
        self._display_system_status()
    
    def _create_sidebar(self):
        """Create sidebar with filters and controls"""
        st.sidebar.header("Filters")
        
        # City selector
        latest_data = self.storage.get_latest_weather()
        cities = list(set(record.city for record in latest_data)) if latest_data else []
        
        selected_city = st.sidebar.selectbox(
            "Select City",
            ["All Cities"] + sorted(cities),
            help="Select a specific city or view all cities"
        )
        
        # Time range selector
        time_range = st.sidebar.selectbox(
            "Time Range",
            ["Last 24 hours", "Last 7 days", "Last 30 days"],
            index=1
        )
        
        # Store selections in session state
        st.session_state.selected_city = selected_city if selected_city != "All Cities" else None
        
        days_mapping = {
            "Last 24 hours": 1,
            "Last 7 days": 7,
            "Last 30 days": 30
        }
        st.session_state.days = days_mapping[time_range]
    
    def _display_overview(self):
        """Display overview metrics"""
        st.header("📊 Overview")
        
        try:
            # Get statistics
            stats = self.storage.get_weather_stats(
                city=st.session_state.selected_city,
                days=st.session_state.days
            )
            
            if stats:
                col1, col2, col3, col4 = st.columns(4)
                
                with col1:
                    st.metric("Cities Monitored", stats.get('cities_covered', 0))
                
                with col2:
                    avg_temp = stats.get('avg_temperature', 0)
                    st.metric("Avg Temperature", f"{avg_temp:.1f}°C")
                
                with col3:
                    avg_humidity = stats.get('avg_humidity', 0)
                    st.metric("Avg Humidity", f"{avg_humidity:.1f}%")
                
                with col4:
                    total_records = stats.get('total_records', 0)
                    st.metric("Total Records", total_records)
            else:
                st.warning("No data available for the selected criteria")
                
        except Exception as e:
            st.error(f"Error loading overview: {e}")
    
    def _display_weather_data(self):
        """Display current weather data"""
        st.header("🌡️ Current Weather")
        
        try:
            latest_data = self.storage.get_latest_weather(st.session_state.selected_city)
            
            if latest_data:
                # Create DataFrame
                df_data = []
                for record in latest_data:
                    df_data.append({
                        'City': record.city,
                        'Country': record.country,
                        'Temperature (°C)': f"{record.temperature_celsius:.1f}",
                        'Feels Like (°C)': f"{record.feels_like - 273.15:.1f}" if record.feels_like else "N/A",
                        'Humidity (%)': record.humidity,
                        'Pressure (hPa)': record.pressure,
                        'Wind Speed (m/s)': f"{record.wind_speed:.1f}" if record.wind_speed else "N/A",
                        'Description': record.description.title(),
                        'Cloudiness (%)': record.cloudiness,
                        'Last Updated': record.timestamp.strftime("%Y-%m-%d %H:%M UTC")
                    })
                
                df = pd.DataFrame(df_data)
                st.dataframe(df, use_container_width=True)
                
            else:
                st.warning("No current weather data available")
                
        except Exception as e:
            st.error(f"Error loading weather data: {e}")
    
    def _display_analytics(self):
        """Display weather analytics and charts"""
        st.header("📈 Weather Analytics")
        
        if not st.session_state.selected_city:
            st.info("Select a specific city from the sidebar to view detailed analytics")
            return
        
        try:
            # Get historical data
            history = self.storage.get_weather_history(
                st.session_state.selected_city, 
                st.session_state.days
            )
            
            if not history:
                st.warning(f"No historical data available for {st.session_state.selected_city}")
                return
            
            # Create DataFrame for plotting
            df = pd.DataFrame([{
                'timestamp': record.timestamp,
                'temperature': record.temperature_celsius,
                'humidity': record.humidity,
                'pressure': record.pressure,
                'wind_speed': record.wind_speed or 0,
                'cloudiness': record.cloudiness
            } for record in history])
            
            df = df.sort_values('timestamp')
            
            # Temperature chart
            col1, col2 = st.columns(2)
            
            with col1:
                fig_temp = px.line(
                    df, 
                    x='timestamp', 
                    y='temperature',
                    title=f'Temperature Trend - {st.session_state.selected_city}',
                    labels={'temperature': 'Temperature (°C)', 'timestamp': 'Time'}
                )
                st.plotly_chart(fig_temp, use_container_width=True)
            
            with col2:
                fig_humidity = px.line(
                    df, 
                    x='timestamp', 
                    y='humidity',
                    title=f'Humidity Trend - {st.session_state.selected_city}',
                    labels={'humidity': 'Humidity (%)', 'timestamp': 'Time'}
                )
                st.plotly_chart(fig_humidity, use_container_width=True)
            
            # Multi-metric chart
            fig_multi = go.Figure()
            
            fig_multi.add_trace(go.Scatter(
                x=df['timestamp'],
                y=df['temperature'],
                mode='lines+markers',
                name='Temperature (°C)',
                yaxis='y'
            ))
            
            fig_multi.add_trace(go.Scatter(
                x=df['timestamp'],
                y=df['humidity'],
                mode='lines+markers',
                name='Humidity (%)',
                yaxis='y2'
            ))
            
            fig_multi.update_layout(
                title=f'Multi-Metric Analysis - {st.session_state.selected_city}',
                xaxis_title='Time',
                yaxis=dict(title='Temperature (°C)', side='left'),
                yaxis2=dict(title='Humidity (%)', side='right', overlaying='y'),
                hovermode='x unified'
            )
            
            st.plotly_chart(fig_multi, use_container_width=True)
            
        except Exception as e:
            st.error(f"Error loading analytics: {e}")
    
    def _display_system_status(self):
        """Display system status and pipeline health"""
        st.header("🔧 System Status")
        
        try:
            # Get pipeline status (simplified version)
            latest_data = self.storage.get_latest_weather()
            
            if latest_data:
                latest_update = max(record.timestamp for record in latest_data)
                time_diff = datetime.utcnow() - latest_update.replace(tzinfo=None)
                
                status_items = [
                    ("Database", "✅ Connected" if latest_data else "❌ No Data"),
                    ("Latest Update", latest_update.strftime("%Y-%m-%d %H:%M UTC")),
                    ("Update Age", f"{time_diff.seconds // 60} minutes ago"),
                    ("Total Cities", len(set(record.city for record in latest_data))),
                    ("Records Today", len([r for r in latest_data 
                                         if r.timestamp.date() == datetime.utcnow().date()]))
                ]
                
                for label, value in status_items:
                    st.info(f"**{label}:** {value}")
            else:
                st.warning("System status unavailable - no data found")
                
        except Exception as e:
            st.error(f"Error loading system status: {e}")

def main():
    """Run the dashboard"""
    dashboard = WeatherDashboard()
    dashboard.run()

if __name__ == "__main__":
    main()
