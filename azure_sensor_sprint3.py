import numpy as np
from azure.iot.device import IoTHubDeviceClient, Message
import time
import json
from datetime import datetime
import matplotlib.pyplot as plt
import pandas as pd

class TemperatureSensor:
    def __init__(self, initial_value, min_value, max_value):
        self.current_value = initial_value
        self.min_value = min_value
        self.max_value = max_value
    
    def simulate(self, mean, variation_level):
        self.current_value = round(self.current_value + np.random.normal(mean, variation_level), 2)
        self.current_value = round_number(self.current_value, self.min_value, self.max_value)
        return self.current_value

class AirHumiditySensor:
    def __init__(self, initial_value, min_value, max_value):
        self.current_value = initial_value
        self.min_value = min_value
        self.max_value = max_value
    
    def simulate(self, mean, variation_level):
        self.current_value = round(self.current_value + np.random.normal(mean, variation_level), 2)
        self.current_value = round_number(self.current_value, self.min_value, self.max_value)
        return self.current_value
    
class SoilHumiditySensor:
    def __init__(self, initial_value, min_value, max_value):
        self.current_value = initial_value
        self.min_value = min_value
        self.max_value = max_value
    
    def simulate(self, mean, variation_level):
        self.current_value = round(self.current_value + np.random.normal(mean, variation_level), 2)
        self.current_value = round_number(self.current_value, self.min_value, self.max_value)
        return self.current_value
    
class Co2Sensor:
    def __init__(self, initial_value, min_value, max_value):
        self.current_value = initial_value
        self.min_value = min_value
        self.max_value = max_value
    
    def simulate(self, mean, variation_level):
        self.current_value = round(self.current_value + np.random.normal(mean, variation_level), 2)
        self.current_value = round_number(self.current_value, self.min_value, self.max_value)
        return self.current_value
    
class AirQualitySensor:
    def __init__(self, initial_value, min_value, max_value):
        self.current_value = initial_value
        self.min_value = min_value
        self.max_value = max_value
    
    def simulate(self, mean, variation_level):
        self.current_value = round(self.current_value + np.random.normal(mean, variation_level), 2)
        self.current_value = round_number(self.current_value, self.min_value, self.max_value)
        return self.current_value

class WindSpeedSensor:
    def __init__(self, initial_value, min_value, max_value):
        self.current_value = initial_value
        self.min_value = min_value
        self.max_value = max_value
    
    def simulate(self, mean, variation_level):
        self.current_value = round(self.current_value + np.random.normal(0, variation_level), 2)
        self.current_value = round_number(self.current_value, self.min_value, self.max_value)
        return self.current_value
    
class WindDirectionSensor:
    def __init__(self, initial_value, min_value, max_value):
        self.current_value = initial_value
        self.min_value = min_value
        self.max_value = max_value

    def simulate(self, mean, variation_level):
        self.current_value = self.current_value + np.random.normal(mean, variation_level)
        self.current_value = round(round_number_wind_direction(self.current_value, self.min_value, self.max_value), 2)
        return self.current_value

def round_number(current_value, min_value, max_value):
    if (current_value < min_value):
        return min_value
    elif (current_value > max_value):
        return max_value
    else:
        return current_value
    
def round_number_wind_direction(current_value, min_value, max_value):
    if (current_value < min_value):
        return 360 + current_value
    elif (current_value > max_value):
        return 0 + (current_value % 360)
    else:
        return current_value

keys = {
    "temperatura": "Temperatura",
    "airHumidity": "Umidade do Ar",
    "soilHumidity": "Umidade do Solo",
    "co2": "CO2",
    "airQuality": "Qualidade do Ar",
    "windSpeed": "Velocidade do Vento",
    "windDirection": "Direção do Vento"
}


def open_iot_hub_connection():
    CONNECTION_STRING = "HostName=EcoFireWatch-IotHub.azure-devices.net;DeviceId=sensor_simulation;SharedAccessKey=RKL/20zuz4b+6/kiRWS8LDU9VNmFAbapYyBYltqtNTQ="
    client = IoTHubDeviceClient.create_from_connection_string(CONNECTION_STRING)
    client.connect()

    return client

def send_iot_hub_message(client, message):
    client.send_message(Message(message))

import threading

def generate_graphs(data, interval, stop_event):
    plt.ion()
    figures = {}
    axis = {}

    for key in keys:
        fig = plt.figure(figsize=(8, 4))
        fig.canvas.manager.set_window_title(keys[key])
        ax = fig.add_subplot(1, 1, 1)
        figures[key] = fig
        axis[key] = ax

    while not stop_event.is_set():
        if not data:
            continue

        df = pd.DataFrame(data)

        for key in keys:
            if len(df) > 1:
                ax = axis[key]
                ax.cla()
                ax.plot(df['insertDate'], df[key], marker='o')
                ax.set_title(keys[key])
                ax.set_xlabel("Data e Hora")

                unidade = {
                    "temperatura": " (°C)",
                    "airHumidity": " (%)",
                    "airSoil": " (%)",
                    "co2": " (ppm)",
                    "airQuality": " (ppm)",
                    "windSpeed": " (m/s)",
                    "windDirection": " (°)"
                }.get(key, "")

                ax.set_ylabel(keys[key] + unidade)
                ax.tick_params(axis='x', rotation=45)
                ax.grid(True)
                figures[key].tight_layout()

        plt.pause(interval)

def begin_simulation(
        interval=10,
        desc_mean=1,
        asc_mean=1,
        variation_level=0.3,
        send_message_azure=False,
        create_csv=False,
        display_graphs=False,
        mass_generation=False,
        jsons_per_second=10,
        csv_separation=";"
        ):
    try:
        if send_message_azure:
            client = open_iot_hub_connection()

        data = []

        stop_event = threading.Event()
        graphs_thread = threading.Thread(
            target=generate_graphs,
            args=(data, interval, stop_event),
            daemon=True
        )
        if display_graphs:
            graphs_thread.start()

        while True:
            start_time = time.time()

            batch_data = []

            for _ in range(jsons_per_second if mass_generation else 1):
                data_tuple = tuple(sensor.simulate(np.random.choice([-desc_mean, asc_mean]), variation_level) for sensor in sensors)

                dictionary = dict(zip(keys, data_tuple))
                dictionary["insertDate"] = datetime.now().isoformat()

                batch_data.append(dictionary)

            for dictionary in batch_data:
                json_str = json.dumps(dictionary, indent=0)

                if send_message_azure:
                    send_iot_hub_message(client, json_str)

                print("Enviando JSON ao Azure\n" + json_str)

            data.extend(batch_data)

            if create_csv:
                df = pd.DataFrame(data)
                df.to_csv("sensor_data.csv", index=False, encoding="utf-8")

            if mass_generation:
                elapsed = time.time() - start_time
                time.sleep(max(0, 1.0 - elapsed))
            else:
                time.sleep(interval)

    except KeyboardInterrupt:
        print("\nSimulação interrompida pelo usuário.")
        if create_csv:
            df = pd.DataFrame(data)
            df.to_csv("sensor_data.csv", index=False, encoding="utf-8", sep=csv_separation)
            print("Arquivo CSV salvo como 'sensor_data.csv'.")
    finally:
        stop_event.set()
        graphs_thread.join()
        if send_message_azure:
            client.disconnect()

sensors = [
    TemperatureSensor(initial_value=25.0, min_value=-200, max_value=600),
    AirHumiditySensor(initial_value=60.0, min_value=0, max_value=100),
    SoilHumiditySensor(initial_value=40.0, min_value=0, max_value=100),
    Co2Sensor(initial_value=4.0, min_value=0, max_value=100),
    AirQualitySensor(initial_value=8.0, min_value=0, max_value=100),
    WindSpeedSensor(initial_value=5.0, min_value=0, max_value=50),
    WindDirectionSensor(initial_value=360.0, min_value=0, max_value=360)
]

if __name__ == "__main__":
    # begin_simulation(
    #     interval=3,
    #     create_csv=True)
    
    begin_simulation(
        mass_generation=True,
        jsons_per_second=100,
        create_csv=True)