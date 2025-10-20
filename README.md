# 🌦️ Global Weather Data Pipeline & Dashboard

![Python](https://img.shields.io/badge/Python-3.11-blue?logo=python)
![Pandas](https://img.shields.io/badge/Pandas-2.1.4-green?logo=pandas)
![BigQuery](https://img.shields.io/badge/Google_BigQuery-4285F4?logo=googlecloud)
![Power BI](https://img.shields.io/badge/Power_BI-F2C811?logo=powerbi)
![Streamlit](https://img.shields.io/badge/Streamlit-FF4B4B?logo=streamlit)

A fully automated **Weather Data Analytics pipeline** that collects live weather data from the **OpenWeatherMap API**, processes it with **Python**, stores it in **Google BigQuery**, visualizes insights in **Power BI**, and deploys a live dashboard using **Streamlit** 

##  Live Demo  
**Streamlit Dashboard:** [weather-data-pipeline.streamlit.app](https://weather-data-pipeline-q59uyqjzuwkpqtsnunuwyg.streamlit.app/)

## Project Overview

This project demonstrates a **real-world ETL + Analytics pipeline**:

1. **Extract** weather data from OpenWeatherMap API for multiple global cities
2. **Transform & Validate** data using Python (pandas)
3. **Load** processed data into Google BigQuery
4. **Visualize** insights using:
   - Power BI (for professional reports)
   - Streamlit (for live, interactive web dashboards)

## Tech Stack

| Stage | Tool | Purpose |
|:------|:------|:---------|
| **Programming** | Python, pandas, requests | Data collection, validation & transformation |
| **Cloud Database** | Google BigQuery | Cloud data warehouse |
| **Visualization** | Power BI Desktop & Web | Business Intelligence reports |
| **Web Dashboard** | Streamlit | Interactive live dashboard |
| **Authentication** | GCP Service Account | Secure cloud access |

**Workflow →** `API → Python ETL → BigQuery → Power BI / Streamlit`

## Project Architecture

```
[OpenWeatherMap API]
        ↓
(Python ETL Script)
        ↓
[Data Validation & Cleaning]
        ↓
[Google BigQuery Warehouse]
        ↓
    ┌─────────────┐
    │   Power BI  │
    │  (Reports)  │
    └─────────────┘
        ↓
    ┌─────────────┐
    │  Streamlit  │
    │ (Live App)  │
    └─────────────┘
```

## 📂 Folder Structure

```
weather-data-pipeline/
├── 📊 streamlit_app.py          # Main dashboard
├── 🔧 weather_pipeline.py       # ETL pipeline
├── 📁 storage/
│   ├── bigquery_loader.py       # BigQuery operations
│   └── data_storage.py          # Data storage handlers
├── 📡 api/
│   └── weather_client.py        # API client
├── ⚙️ processing/
│   └── data_validator.py        # Data validation
├── 📋 requirements.txt          # Dependencies
├── 🔒 .gitignore                # Security
└── 📖 README.md                 # This file
```

## How to Run This Project Locally

### 1️ Clone the repository
```bash
git clone https://github.com/alfiya-anjum/weather-data-pipeline.git
cd weather-data-pipeline
```

### 2️ Set up environment
```bash
python -m venv venv
# Windows:
venv\Scripts\activate
# Mac/Linux:
source venv/bin/activate

pip install -r requirements.txt
```

### 3️ Configure environment variables
Create a `.env` file:
```env
OPENWEATHER_API_KEY=your_api_key_here
GCP_PROJECT_ID=your_project_id
GCP_DATASET_ID=weather_data
```

### 4️ Add GCP credentials
Create `gcp_key.json` in project root with your service account JSON.

> ⚠️ **Security Note:** Never commit this file to GitHub!

### 5️ Run the ETL pipeline
```bash
python weather_pipeline.py --mode single
```

### 6️ Launch the dashboard
```bash
streamlit run streamlit_app.py
```
Open → [http://localhost:8501](http://localhost:8501) 🌈

---

## Dashboard Features

### Power BI Reports
- 🌡 **Temperature trends** over time
- 💧 **Humidity averages** by city  
- 🌦 **Weather condition** distribution
- 💨 **Wind speed** vs humidity analysis

  <img width="1182" height="675" alt="Screenshot 2025-10-20 120403" src="https://github.com/user-attachments/assets/10412e51-34c9-4ecd-bd55-88154d2463b8" />


### Streamlit Dashboard
-  **Real-time weather** for major cities
-  **Interactive charts** and metrics
-  **City comparison** tools
-  **Live data updates**

<img width="1794" height="833" alt="image" src="https://github.com/user-attachments/assets/b5157fa4-8556-4bc7-b2c6-3866b2f1dd39" />
<img width="1834" height="836" alt="image" src="https://github.com/user-attachments/assets/8662e6b9-7f07-4114-9f65-3bb4bf1c4f9e" />
<img width="1803" height="838" alt="image" src="https://github.com/user-attachments/assets/02fb72b1-c5b4-4c36-aae2-fffbbc5459ef" />
<img width="1841" height="818" alt="image" src="https://github.com/user-attachments/assets/dc80d1e0-a2a8-4517-8308-bf2d69b7f9ad" />

## Deployment

This project is deployed on **Streamlit Cloud**:
👉 [weather-data-pipeline.streamlit.app](https://weather-data-pipeline-q59uyqjzuwkpqtsnunuwyg.streamlit.app/)

**Deployment features:**
- ✅ Automatic deployments from GitHub
- ✅ Secure environment variables
- ✅ 24/7 availability
- ✅ Scalable cloud infrastructure

---

## Example Pipeline Output

<img width="1339" height="690" alt="Screenshot 2025-10-20 094627" src="https://github.com/user-attachments/assets/93b03c8f-aef3-4434-8140-0d09b61d8aee" />


## Future Improvements

- [ ] **Automated scheduling** with Cloud Scheduler/Airflow
- [ ] **Weather alerts** for drastic changes
- [ ] **Geospatial maps** with city locations  
- [ ] **Historical trend** analysis (30-day)
- [ ] **Machine learning** weather predictions
- [ ] **Multi-language** support


## 👩‍💻 Author

**Alfiya Anjum**  

## Acknowledgements

- [OpenWeatherMap API](https://openweathermap.org/api) for weather data
- [Google BigQuery](https://cloud.google.com/bigquery) for cloud data warehouse
- [Streamlit](https://streamlit.io) for amazing dashboard framework
- [Power BI](https://powerbi.microsoft.com/) for business intelligence

<div align="center">

**⭐ Don't forget to star this repo if you found it helpful!**

*Built with ❤️ and ☕ by Alfiya Anjum*

</div>


### Quick Links
- [Report Bug](https://github.com/alfiya-anjum/weather-data-pipeline/issues)
- [Request Feature](https://github.com/alfiya-anjum/weather-data-pipeline/issues)
- [View Code](https://github.com/alfiya-anjum/weather-data-pipeline)



