import numpy as np
from azure.iot.device import IoTHubDeviceClient, Message
import time
import json
from datetime import datetime
import matplotlib.pyplot as plt
import pandas as pd

class TemperatureSensor:
    def __init__(self, initial_value, min_value, max_value):
        self.currente_value = initial_value
        self.min_value = min_value
        self.max_value = max_value
    
    def simulate(self, variation_level):
        self.currente_value = round(self.currente_value + np.random.normal(-variation_level, variation_level), 2)
        self.current_value = round_number(self.currente_value, self.min_value, self.max_value)
        return self.currente_value

class AirHumiditySensor:
    def __init__(self, initial_value, min_value, max_value):
        self.currente_value = initial_value
        self.min_value = min_value
        self.max_value = max_value
    
    def simulate(self, variation_level):
        self.currente_value = round(self.currente_value + np.random.normal(-variation_level, variation_level), 2)
        self.current_value = round_number(self.currente_value, self.min_value, self.max_value)
        return self.currente_value
    
class SoilHumiditySensor:
    def __init__(self, initial_value, min_value, max_value):
        self.currente_value = initial_value
        self.min_value = min_value
        self.max_value = max_value
    
    def simulate(self, variation_level):
        self.currente_value = round(self.currente_value + np.random.normal(-variation_level, variation_level), 2)
        self.current_value = round_number(self.currente_value, self.min_value, self.max_value)
        return self.currente_value
    
class Co2Sensor:
    def __init__(self, initial_value, min_value, max_value):
        self.currente_value = initial_value
        self.min_value = min_value
        self.max_value = max_value
    
    def simulate(self, variation_level):
        self.currente_value = round(self.currente_value + np.random.normal(-variation_level, variation_level), 2)
        self.current_value = round_number(self.currente_value, self.min_value, self.max_value)
        return self.currente_value
    
class AirQualitySensor:
    def __init__(self, initial_value, min_value, max_value):
        self.currente_value = initial_value
        self.min_value = min_value
        self.max_value = max_value
    
    def simulate(self, variation_level):
        self.currente_value = round(self.currente_value + np.random.normal(-variation_level, variation_level), 2)
        self.current_value = round_number(self.currente_value, self.min_value, self.max_value)
        return self.currente_value

class WindSpeedSensor:
    def __init__(self, initial_value, min_value, max_value):
        self.currente_value = initial_value
        self.min_value = min_value
        self.max_value = max_value
    
    def simulate(self, variation_level):
        self.currente_value = round(self.currente_value + np.random.normal(0, variation_level), 2)
        self.current_value = round_number(self.currente_value, self.min_value, self.max_value)
        return self.currente_value
    
class WindDirectionSensor:
    def __init__(self, initial_value, variation_level):
        self.current_value = initial_value % 360
        self.variation_level = variation_level

    def simulate(self, variation_level):
        variation = np.random.normal(0, self.variation_level)
        self.current_value = (self.current_value + variation) % 360
        return round(self.current_value, 2)

def round_number(current_value, min_value, max_value):
    if (current_value < min_value):
        return min_value
    elif (current_value > max_value):
        return max_value
    else:
        return current_value

sensors = [
    TemperatureSensor(25.0, -200, 600),
    AirHumiditySensor(60.0, 0, 100),
    SoilHumiditySensor(40.0, 0, 100),
    Co2Sensor(4.0, 0, 100),
    AirQualitySensor(8.0, 0, 100),
    WindSpeedSensor(5.0, 0, 50),
    WindDirectionSensor(np.random.uniform(0, 360), 10)
]

keys = {
    "temperatura": "Temperatura",
    "airHumidity": "Umidade do Ar",
    "airSoil": "Umidade do Solo",
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



def begin_simulation(interval=10):
    try:
        client = open_iot_hub_connection()
        data = []

        stop_event = threading.Event()
        graphs_thread = threading.Thread(
            target=generate_graphs,
            args=(data, interval, stop_event),
            daemon=True
        )
        graphs_thread.start()

        while True:
            data_tuple = tuple(sensor.simulate(0.5) for sensor in sensors)

            dictionary = dict(zip(keys, data_tuple))
            dictionary["insertDate"] = datetime.now().isoformat()
            dictionaryGraph = dict(zip(keys, data_tuple))
            dictionaryGraph["insertDate"] = datetime.now().strftime("%H:%M:%S")
            json_str = json.dumps(dictionary, indent=2)

            # send_iot_hub_message(client, json_str)
            print("Enviando JSON ao Azure\n" + json_str)
            data.append(dictionary)

            time.sleep(interval)

    except KeyboardInterrupt:
        print("\nSimulação interrompida pelo usuário.")
        stop_event.set()
        graphs_thread.join()
        if client:
            client.disconnect()
    finally:
        if client:
            client.disconnect()

if __name__ == "__main__":
    begin_simulation()