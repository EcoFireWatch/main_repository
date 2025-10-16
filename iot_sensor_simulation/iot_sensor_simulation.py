import numpy as np
import time
import json
from datetime import datetime
import boto3
from botocore.exceptions import NoCredentialsError, ClientError
import pandas as pd
import requests

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

# ---------------------- Função para enviar JSON ao S3 ----------------------
def upload_json_to_s3(json_data, bucket_name, s3_key):
    s3 = boto3.Session(profile_name="default").client('s3')
    try:
        s3.put_object(
            Bucket=bucket_name,
            Key=s3_key,
            Body=json.dumps(json_data, indent=0)
        )
        print(f"JSON enviado para S3: s3://{bucket_name}/{s3_key}")
    except NoCredentialsError:
        print("Credenciais AWS não encontradas.")
    except ClientError as e:
        print(f"Erro ao enviar JSON: {e}")

# ---------------------- Simulação ----------------------
def begin_simulation(
    farm_id=1,
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
    jsons_per_second=10,
    csv_separation=";",
    upload_to_s3=False,
    s3_bucket_name="",
    s3_key_prefix="iot_sensor/"
):
    try:
        # Criação dinâmica de sensores com IDs únicos
        sensors_config = {
            "temperature": (TemperatureSensor, n_temperature_sensors, 25.0, -200, 600),
            "airHumidity": (AirHumiditySensor, n_air_humidity_sensors, 60.0, 0, 100),
            "soilHumidity": (SoilHumiditySensor, n_soil_humidity_sensors, 40.0, 0, 100),
            "co2": (Co2Sensor, n_co2_sensors, 4.0, 0, 100),
            "airQuality": (AirQualitySensor, n_air_quality_sensors, 8.0, 0, 100),
            "windSpeed": (WindSpeedSensor, n_wind_speed_sensors, 5.0, 0, 50),
            "windDirection": (WindDirectionSensor, n_wind_direction_sensors, 360.0, 0, 360),
        }

        sensors = {}
        sensor_id_counter = 1

        for key, (cls, n, init, minv, maxv) in sensors_config.items():
            sensors[key] = [
                {"sensor": cls(init, minv, maxv), "sensorId": sensor_id_counter + i}
                for i in range(n)
            ]
            sensor_id_counter += n

        data = []

        while True:
            start_time = time.time()
            batch_data = []

            for _ in range(jsons_per_second if mass_generation else 1):
                result = {"farmId": farm_id, "insertDate": datetime.now().isoformat()}

                for key in sensors.keys():
                    sensor_list = []
                    for s in sensors[key]:
                        sensor_obj = s["sensor"]
                        sensor_id = s["sensorId"]
                        value = sensor_obj.simulate(np.random.choice([-desc_mean, asc_mean]), variation_level)
                        sensor_list.append({"value": value, "sensorId": sensor_id})
                    result[key] = sensor_list

                batch_data.append(result)

            json_batch = batch_data[0]
            print("JSON:\n" + json.dumps(json_batch, indent=4))

            data.extend(batch_data)

            # Enviar para API Gateway
            if send_to_api_gateway and not mass_generation:
                requests.post(url=api_gateway_url, json=json_batch)

            # Salvar CSV opcional
            if create_csv:
                df = pd.DataFrame([{
                    "insertDate": d["insertDate"],
                    **{k: d[k][0]["value"] for k in keys}
                } for d in data])
                df.to_csv("sensor_data.csv", index=False, encoding="utf-8", sep=csv_separation)

            # Enviar JSON para S3
            if upload_to_s3:
                timestamp = datetime.now().strftime("%Y-%m-%dT%H-%M-%S")
                s3_key = f"{s3_key_prefix}farm_{farm_id}_{timestamp}.json"
                upload_json_to_s3(json_batch, s3_bucket_name, s3_key)

            if mass_generation:
                elapsed = time.time() - start_time
                time.sleep(max(0, 1.0 - elapsed))
            else:
                time.sleep(interval)

    except KeyboardInterrupt:
        print("\nSimulação interrompida pelo usuário.")
        if create_csv:
            df = pd.DataFrame([{
                "insertDate": d["insertDate"],
                **{k: d[k][0]["value"] for k in keys}
            } for d in data])
            df.to_csv("sensor_data.csv", index=False, encoding="utf-8", sep=csv_separation)
            print("Arquivo CSV salvo como 'sensor_data.csv'.")

# ---------------------- Execução ----------------------
if __name__ == "__main__":
    begin_simulation(
        farm_id=1,
        n_temperature_sensors=4,
        n_air_humidity_sensors=4,
        n_soil_humidity_sensors=4,
        n_co2_sensors=4,
        n_air_quality_sensors=4,
        n_wind_speed_sensors=4,
        n_wind_direction_sensors=4,
        interval=3,
        create_csv=False,
        upload_to_s3=True,
        s3_bucket_name="eco-fire-watch-test-trusted"
    )
