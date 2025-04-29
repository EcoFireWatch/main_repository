import numpy as np
from azure.iot.device import IoTHubDeviceClient, Message
import time
import json
from datetime import datetime

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
        self.currente_value = round(self.currente_value + np.random.normal(-variation_level, variation_level), 2)
        self.current_value = round_number(self.currente_value, self.min_value, self.max_value)
        return self.currente_value
    
class WindDirectionSensor:
    def __init__(self, initial_value, min_value, max_value):
        self.currente_value = initial_value
        self.min_value = min_value
        self.max_value = max_value
    
    def simulate(self, variation_level):
        self.currente_value = round(self.currente_value + np.random.normal(-variation_level, variation_level), 2)
        self.current_value = round_number(self.currente_value, self.min_value, self.max_value)
        return self.currente_value

sensors = [
    TemperatureSensor(25.0, -200, 600),
    AirHumiditySensor(60.0, 0, 100),
    SoilHumiditySensor(40.0, 0, 100),
    Co2Sensor(4.0, 0, 100),
    AirQualitySensor(8.0, 0, 100),
    WindSpeedSensor(5.0, 0, 200),
    WindDirectionSensor(0, 0, 360)
]

def open_iot_hub_connection():
    CONNECTION_STRING = "HostName=EcoFireWatch-IotHub.azure-devices.net;DeviceId=sensor_simulation;SharedAccessKey=RKL/20zuz4b+6/kiRWS8LDU9VNmFAbapYyBYltqtNTQ="
    client = IoTHubDeviceClient.create_from_connection_string(CONNECTION_STRING)
    client.connect()

    return client

def send_iot_hub_message(client, message):
    client.send_message(Message(message))

def round_number(current_value, min_value, max_value):
    if (current_value < min_value):
        return min_value
    elif (current_value > max_value):
        return max_value
    else:
        return

def begin_simulation():
    try:
        client = open_iot_hub_connection()

        while (True):
            data_tuple = tuple(sensor.simulate(0.5) for sensor in sensors)
            keys = ["temperatura", "airHumidity", "airSoil", "co2", "airQuality", "windSpeed", "windDirection"]

            dictionary = dict(zip(keys, data_tuple))
            dictionary["insertDate"] = datetime.now().isoformat()
            json_str = json.dumps(dictionary, indent=2)

            send_iot_hub_message(client, json_str)
            print("Enviando JSON ao Azure\n" + json_str)

            time.sleep(10)

    except KeyboardInterrupt:
        if client:
            client.disconnect()
    finally:
        if client:
            client.disconnect()


if __name__ == "__main__":
    begin_simulation()
