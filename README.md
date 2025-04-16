# iot-sensor-data-spark-sql

# IoT Sensor Data Analytics with Spark SQL

This project analyzes IoT sensor data using Apache Spark SQL. It covers tasks like data loading, filtering, time-based analysis, sensor ranking, and pivoting — all while saving results to CSV files for further insights.

# Dataset

The dataset used is `sensor_data.csv`, containing readings from various IoT sensors across different building locations and times.

**Sample Columns:**
- `sensor_id` (int)
- `timestamp` (string → timestamp)
- `temperature` (float)
- `humidity` (float)
- `location` (string)
- `sensor_type` (string)

---

# Tasks & Outputs

| Task | Description | Output |
|------|-------------|--------|
| **Task 1** | Load data, explore schema, basic queries | `task1_output.csv` |
| **Task 2** | Filter temperature, aggregate by location | `task2_output.csv` |
| **Task 3** | Analyze temperature trends by hour of day | `task3_output.csv` |
| **Task 4** | Rank sensors by average temperature using window functions | `task4_output.csv` |
| **Task 5** | Create pivot table by location and hour | `task5_output.csv` |

---

# How to Run

1. Make sure you have Spark installed and `spark-submit` available.
2. Place `sensor_data.csv` in the same directory as the script.
3. Run the script: `spark-submit iot_sensor_analytics.py`

# Issues

1. I didnt have much issue with this one, the iot_sensor_analytics.py needed some tweaks but nothing that kept me stuck for as long as some previous assignments. The results all printed correctly too.