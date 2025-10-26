import numpy as np
import time
import json
from datetime import datetime
import boto3
from botocore.exceptions import NoCredentialsError, ClientError
import pandas as pd
import requests
import psycopg2

# ---------------------- Sensores ----------------------
class BaseSensor:
    def __init__(self, initial_value, min_value, max_value):
        self.current_value = initial_value
        self.min_value = min_value
        self.max_value = max_value

    def simulate(self, mean, variation_level):
        self.current_value = round(self.current_value + np.random.normal(mean, variation_level), 2)
        return self._round_value(self.current_value)

    def _round_value(self, current_value):
        if current_value < self.min_value:
            return self.min_value
        elif current_value > self.max_value:
            return self.max_value
        return current_value

class TemperatureSensor(BaseSensor): pass
class AirHumiditySensor(BaseSensor): pass
class SoilHumiditySensor(BaseSensor): pass
class Co2Sensor(BaseSensor): pass
class AirQualitySensor(BaseSensor): pass
class WindSpeedSensor(BaseSensor): pass

class WindDirectionSensor(BaseSensor):
    def _round_value(self, current_value):
        if current_value < self.min_value:
            return 360 + current_value
        elif current_value > self.max_value:
            return current_value % 360
        return current_value

# ---------------------- Chaves ----------------------
keys = [
    "temperature",
    "airHumidity",
    "soilHumidity",
    "co2",
    "airQuality",
    "windSpeed",
    "windDirection"
]

# ---------------------- Funções de envio ----------------------
def upload_json_batch_to_s3(json_batch, bucket_name, s3_key):
    s3 = boto3.Session(profile_name="default").client('s3')
    try:
        s3.put_object(
            Bucket=bucket_name,
            Key=s3_key,
            Body=json.dumps(json_batch, indent=0)
        )
        print(f"Batch JSON enviado para S3: s3://{bucket_name}/{s3_key}")
    except NoCredentialsError:
        print("Credenciais AWS não encontradas.")
    except ClientError as e:
        print(f"Erro ao enviar JSON: {e}")

def upload_json_batch_to_postgres(json_batch, connection_params):
    unit_measure_map = {
        "temperature": 1,
        "airHumidity": 2,
        "soilHumidity": 2,
        "co2": 3,
        "airQuality": 4,
        "windSpeed": 5,
        "windDirection": 6
    }

    records = []
    for json_data in json_batch:
        farm_id = json_data["farmId"]
        timestamp = json_data["insertDate"]
        for sensor_type, sensor_list in json_data.items():
            if sensor_type not in unit_measure_map:
                continue
            unit_measure_id = unit_measure_map[sensor_type]
            for s in sensor_list:
                sensor_id = s["sensorId"]
                value = s["value"]
                records.append((sensor_id, farm_id, unit_measure_id, timestamp, value))

    if not records:
        return

    try:
        conn = psycopg2.connect(**connection_params)
        cur = conn.cursor()
        cur.executemany("""
            INSERT INTO fact (sensor_id, farm_id, unit_measure_id, timestamp, value)
            VALUES (%s, %s, %s, %s, %s)
        """, records)
        conn.commit()
        print(f"{len(records)} registros inseridos no PostgreSQL")
    except Exception as e:
        print(f"Erro ao inserir batch no PostgreSQL: {e}")
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()

# ---------------------- Simulação ----------------------
def begin_simulation(
    farm_id=4,
    n_temperature_sensors=1,
    n_air_humidity_sensors=1,
    n_soil_humidity_sensors=1,
    n_co2_sensors=1,
    n_air_quality_sensors=1,
    n_wind_speed_sensors=1,
    n_wind_direction_sensors=1,
    send_to_api_gateway=False,
    api_gateway_url="",
    interval=10,
    desc_mean=1,
    asc_mean=1,
    variation_level=0.3,
    create_csv=False,
    mass_generation=False,
    mass_generation_interval_seconds=3,
    jsons_per_second=10,
    csv_separation=";",
    upload_to_s3=False,
    s3_bucket_name="",
    s3_key_prefix="iot_sensor/",
    send_to_postgres=False,
    postgres_connection_params=None,
    batch_size=50
):
    try:
        # Configuração dinâmica dos sensores
        sensors_config = {
            "temperature": (TemperatureSensor, n_temperature_sensors, 25.0, 5, 100),
            "airHumidity": (AirHumiditySensor, n_air_humidity_sensors, 60.0, 0, 100),
            "soilHumidity": (SoilHumiditySensor, n_soil_humidity_sensors, 40.0, 0, 100),
            "co2": (Co2Sensor, n_co2_sensors, 400.0, 300, 6000),
            "airQuality": (AirQualitySensor, n_air_quality_sensors, 8.0, 0, 150),
            "windSpeed": (WindSpeedSensor, n_wind_speed_sensors, 5.0, 0, 50),
            "windDirection": (WindDirectionSensor, n_wind_direction_sensors, 180.0, 0, 360),
        }

        sensors = {}
        sensor_id_counter = 1
        for key, (cls, n, init, minv, maxv) in sensors_config.items():
            sensors[key] = [{"sensor": cls(init, minv, maxv), "sensorId": sensor_id_counter + i} for i in range(n)]
            sensor_id_counter += n

        # CSV
        if create_csv:
            csv_file = open("sensor_data.csv", "w", encoding="utf-8")
            header = ["insertDate"] + keys
            csv_file.write(csv_separation.join(header) + "\n")

        simulated_time = datetime.now()
        json_batch = []

        while True:
            start_time = time.time()

            for _ in range(jsons_per_second if mass_generation else 1):
                if mass_generation:
                    simulated_time += pd.to_timedelta(mass_generation_interval_seconds, unit='s')
                else:
                    simulated_time += pd.to_timedelta(interval, unit='s')

                result = {"farmId": farm_id, "insertDate": simulated_time.isoformat()}

                for key in sensors.keys():
                    sensor_list = []
                    for s in sensors[key]:
                        sensor_obj = s["sensor"]
                        sensor_id = s["sensorId"]
                        value = round(sensor_obj.simulate(np.random.choice([-desc_mean, asc_mean]), variation_level), 2)
                        sensor_list.append({"value": value, "sensorId": sensor_id})
                    result[key] = sensor_list

                json_batch.append(result)

                # Envio para API Gateway
                if send_to_api_gateway and api_gateway_url:
                    try:
                        requests.post(url=api_gateway_url, json=result)
                    except Exception as e:
                        print(f"Erro ao enviar para API Gateway: {e}")

                # CSV
                if create_csv:
                    row = [result["insertDate"]] + [result[k][0]["value"] for k in keys]
                    csv_file.write(csv_separation.join(map(str, row)) + "\n")

                print(json.dumps(result, indent=4))

            # Batch para PostgreSQL
            if send_to_postgres and postgres_connection_params and len(json_batch) >= batch_size:
                upload_json_batch_to_postgres(json_batch, postgres_connection_params)

            # Batch para S3
            if upload_to_s3 and len(json_batch) >= batch_size:
                timestamp_start = json_batch[0]["insertDate"].replace(":", "-").replace(".", "-")
                timestamp_end = json_batch[-1]["insertDate"].replace(":", "-").replace(".", "-")
                s3_key = f"{s3_key_prefix}farm_{farm_id}_{timestamp_start}_to_{timestamp_end}.json"
                upload_json_batch_to_s3(json_batch, s3_bucket_name, s3_key)

            if (upload_to_s3 or send_to_postgres) and len(json_batch) >= batch_size:
                json_batch.clear()

            # Sleep
            sleep_time = interval if not mass_generation else 0
            elapsed = time.time() - start_time
            time.sleep(max(0, sleep_time - elapsed))

    except KeyboardInterrupt:
        print("\nSimulação interrompida pelo usuário.")
        if create_csv:
            csv_file.close()
            print("Arquivo CSV salvo como 'sensor_data.csv'")

        if send_to_postgres and json_batch and postgres_connection_params:
            upload_json_batch_to_postgres(json_batch, postgres_connection_params)
        if upload_to_s3 and json_batch:
            timestamp_start = json_batch[0]["insertDate"].replace(":", "-").replace(".", "-")
            timestamp_end = json_batch[-1]["insertDate"].replace(":", "-").replace(".", "-")
            s3_key = f"{s3_key_prefix}farm_{farm_id}_{timestamp_start}_to_{timestamp_end}.json"
            upload_json_batch_to_s3(json_batch, s3_bucket_name, s3_key)

# ---------------------- Execução ----------------------
if __name__ == "__main__":
    postgres_config = {
        "host": "eco-fire-watch-client.co6mdrjhs9iz.us-east-1.rds.amazonaws.com",
        "port": 5432,
        "dbname": "postgres",
        "user": "efwUserDatabase",
        "password": "groupoEfw2025"
    }

    begin_simulation(
        farm_id=4,
        n_temperature_sensors=4,
        n_air_humidity_sensors=4,
        n_soil_humidity_sensors=4,
        n_co2_sensors=4,
        n_air_quality_sensors=4,
        n_wind_speed_sensors=4,
        n_wind_direction_sensors=4,
        interval=3,
        mass_generation=True,
        mass_generation_interval_seconds=10,
        jsons_per_second=200,
        send_to_postgres=True,
        postgres_connection_params=postgres_config,
        upload_to_s3=True,
        s3_bucket_name="eco-fire-watch-test-trusted",
        batch_size=200
    )

# Teste CI/CD 3