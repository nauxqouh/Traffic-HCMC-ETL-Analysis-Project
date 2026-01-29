# Data Engineer Final Project: Smart Traffic HCMC

## Folder Structure

```bash
Traffic-HCMC-ETL/
├── docker-compose.yml       # File điều khiển toàn bộ hệ thống
├── Dockerfile               # Để build image Spark có sẵn thư viện minio
├── .gitignore               
├── jars/                    # Nơi chứa các file .jar
│   ├── hadoop-aws-3.3.4.jar
│   ├── aws-java-sdk-bundle-1.12.262.jar
│   └── postgresql-42.7.8.jar
├── spark-data/               
│   └── input/               # Chứa 2 file JSON 4GB 
├── spark-apps/              # Nơi chứa toàn bộ logic xử lý (Gắn vào /opt/spark-apps)
│   ├── bronze/              # Các script nạp data thô
│   │   └── traffic_bronze_ingest.py
│   ├── silver/              # Các script làm sạch, ép kiểu
│   │   └── ..
│   └── gold/                # Các script tổng hợp, đẩy vào Postgres
│       └── ..
├── conf/  
│   └── spark-defaults.conf  # Cấu hình Spark (nếu cần)
└── README.md                # Hướng dẫn chạy đồ án
```

## Setup

**Step 1:** Clone this repository:

```bash
git clone https://github.com/nauxqouh/Traffic-HCMC-ETL-Analysis-Project.git
```

**Step 2:** Ensure that `spark-data/input` and `jars/` folder structure as above. 

Download here: 
- [jars download](https://drive.google.com/drive/folders/1lppS2eHXyM_IdzRfN-xD4AlSHx1A-7Fh?usp=sharing)
- [Traffic Data](https://www.kaggle.com/datasets/ren294/iot-car-hcmcity)


**Step 3:** Build and start container:
```bash
docker compose up -d
```

## Run Spark

```bash
docker exec -it spark-master /opt/spark/bin/spark-submit \
--master spark://spark-master:7077 \
/opt/spark-apps/bronze/traffic_bronze_ingest.py
```

```bash
docker exec -it spark-master /opt/spark/bin/spark-submit \
--master spark://spark-master:7077 \
--executor-memory 4g \
--driver-memory 2g \
--conf spark.executor.memoryOverhead=1g \
/opt/spark-apps/silver/bronze_to_silver.py
```

```bash
docker exec -it spark-master /opt/spark/bin/spark-submit \
--master spark://spark-master:7077 \
--executor-memory 4g \
--driver-memory 2g \
--conf spark.executor.memoryOverhead=1g \
/opt/spark-apps/gold/silver_to_gold.py
```

Get into: http://localhost:9001/ to check the results.

### 📌 TODO

`Gold layer` next!!


## Data Source

The dataset provided describes information about a vehicle (in this case, a motorbike) moving along a specific road in Ho Chi Minh City. It includes various details about the vehicle, its owner, weather conditions, traffic status, and alerts related to the vehicle during its journey. This data can be used in traffic monitoring systems, vehicle operation analysis, or smart transportation services.

**Link source:** [Kaggle - IOT RoadTransport HCMCity](https://www.kaggle.com/datasets/ren294/iot-car-hcmcity)

**Key Data Fields:**

- `vehicle_id`: ID of the vehicle.
- `owner`: Information about the vehicle owner:
- `name`: Owner's name.
- `license_number`: Vehicle's license plate number.
- `contact_info`: Contact details of the owner (phone number, email).
- `speed_kmph`: The vehicle's speed (km/h).
- `road`: Information about the road the vehicle is traveling on:
- `street`: Street name.
- `district`: District of the city.
- `city`: The city (Ho Chi Minh City).
- `timestamp`: The timestamp when the data is recorded.
- `vehicle_size`: Dimensions of the vehicle:
- `length_meters`: Vehicle length (meters).
- `width_meters`: Vehicle width (meters).
- `height_meters`: Vehicle height (meters).
- `vehicle_type`: Type of vehicle (e.g., motorbike).
- `vehicle_classification`: Classification of the vehicle (e.g., civilian).
- `coordinates`: Location of the vehicle (latitude and longitude).
- `engine_status`: Status of the vehicle's engine:
- `is_running`: Whether the engine is running.
- `rpm`: Engine RPM (revolutions per minute).
- `oil_pressure`: Oil pressure status (e.g., "Normal").
- `fuel_level_percentage`: The fuel level percentage in the vehicle.
- `passenger_count`: The number of passengers in the vehicle.
- `internal_temperature_celsius`: Internal temperature inside the vehicle (°C).
- `weather_condition`: Weather conditions at the time of travel:
- `temperature_celsius`: External temperature (°C).
- `humidity_percentage`: Humidity level (%).
- `condition`: Weather condition (e.g., "Clear").
- `estimated_time_of_arrival`: Estimated time of arrival at the destination:
- `destination`: Destination address.
- `eta`: Estimated time of arrival.
- `traffic_status`: Traffic status on the road:
- `congestion_level`: Traffic congestion level (e.g., "Moderate").
- `estimated_delay_minutes`: Estimated delay in minutes.
- `alerts`: Alerts related to the vehicle:
- `type`: Type of alert (e.g., "Speeding").
- `description`: Description of the alert.
- `severity`: Severity of the alert (e.g., "Medium").
- `timestamp`: The timestamp when the alert occurred.

**JSON Schema Description:**

```JSON
{
    "vehicle_id": "string",
    "owner": {
        "name": "string",
        "license_number": "string",
        "contact_info": {
            "phone": "string",
            "email": "string"
        }
    },
    "speed_kmph": "float",
    "road": {
        "street": "string",
        "district": "string",
        "city": "string"
    },
    "timestamp": "string",
    "vehicle_size": {
        "length_meters": "float",
        "width_meters": "float",
        "height_meters": "float"
    },
    "vehicle_type": "string",
    "vehicle_classification": "string",
    "coordinates": {
        "latitude": "float",
        "longitude": "float"
    },
    "engine_status": {
        "is_running": "boolean",
        "rpm": "int",
        "oil_pressure": "string"
    },
    "fuel_level_percentage": "int",
    "passenger_count": "int",
    "internal_temperature_celsius": "float",
    "weather_condition": {
        "temperature_celsius": "float",
        "humidity_percentage": "float",
        "condition": "string"
    },
    "estimated_time_of_arrival": {
        "destination": {
            "street": "string",
            "district": "string",
            "city": "string"
        },
        "eta": "string"
    },
    "traffic_status": {
        "congestion_level": "string",
        "estimated_delay_minutes": "int"
    },
    "alerts": [
        {
            "type": "string",
            "description": "string",
            "severity": "string",
            "timestamp": "string"
        }
    ]
}
```

**Data Sample:**

```JSON
{
    "vehicle_id":"VH23418",
    "owner":{
        "name":"Nguyen Van L",
        "license_number":"91A-99044",
        "contact_info":{
            "phone":"+849129533722",
            "email":"nguyen van j@example.com"
        }
    },
    "speed_kmph":58.968485888881574,
    "road":{
        "street":"Duong Cach Mang Thang Tam",
        "district":"Quan 3",
        "city":"TP Ho Chi Minh",
    },
    "timestamp":"2024-04-01 00:00:00",
    "vehicle_size":{
        "length_meters":2.194526621348187,
        "width_meters":0.7129273918209509,
        "height_meters":1.134440422658781,
    },
    "vehicle_type":"Motorbike",
    "vehicle_classification":"Civilian",
    "coordinates":{
        "latitude":10.767187972712605,
        "longitude":106.67092667652287
    },
    "engine_status":{
        "is_running":true,
        "rpm":2504,
        "oil_pressure":"Normal"
    },
    "fuel_level_percentage":72,
    "passenger_count":5,
    "internal_temperature_celsius":28.35848249735346,
    "weather_condition":{
        "temperature_celsius":32.26467608456345,
        "humidity_percentage":59.98753156458649,
        "condition":"Clear"
    },
    "estimated_time_of_arrival":{
        "destination":{
            "street":"Cau Sai Gon",
            "district":"Quan 5",
            "city":"TP Ho Chi Minh",
        },
        "eta":"2024-04-01 01:03:00",
    },
    "traffic_status":{
        "congestion_level":"Low",
        "estimated_delay_minutes":0,
    },
    "alerts":[
        {
            "type":"Speeding",
            "description":"Fuel level is below 20%",
            "severity":"Medium",
            "timestamp":"2024-03-31 23:51:00"
        }
    ]
}
```

Reference to get idea:
- [TraffictoSilverLayer](https://github.com/Ren294/SmartTraffic_Lakehouse_for_HCMC/blob/main/spark/apps/streaming/TrafficDataToSilverLayer.py)
- [Gold Layer Processing](https://github.com/Ren294/SmartTraffic_Lakehouse_for_HCMC/tree/main/spark/apps/gold)
