import numpy as np
import time
import tracemalloc
import psutil
from concurrent.futures import ThreadPoolExecutor, as_completed
import multiprocessing
import datetime
import json
import boto3
import os

s3 = boto3.client('s3')
s3_name = 'eco-firewatch-raw'

class Sensor:
    def __init__(self, nome, valor_inicial):
        self.nome = nome
        self.valor = valor_inicial

    def medir(self):
        self.valor = round(self.valor + np.random.normal(-0.5, 0.5), 2)
        return self.valor

class SensorDirecaoVento:
    def __init__(self, nome, valor_inicial):
        self.nome = nome
        self.valor = valor_inicial

    def medir(self):
        self.valor = round(self.valor + np.random.uniform(-1,1)) % 360
        if self.valor < 0:
            self.valor = 360 - self.valor
        return self.valor
class SensorUmidadeCaotico:
    def __init__(self, nome, valor_inicial):
        self.nome = nome
        self.valor = valor_inicial

    def medir(self):
        self.valor = round(self.valor + np.random.normal(-20, 20), 2)
        return self.valor

class SensorVentoSuperEstavel:
    def __init__(self, nome, valor_inicial):
        self.nome = nome
        self.valor = valor_inicial

    def medir(self):
        self.valor = round(self.valor + np.random.normal(-0.001, 0.001), 2)
        return self.valor

sensors = [
    Sensor("temperatura", 25.0),
    SensorUmidadeCaotico("umidade_ar", 60.0),
    Sensor("umidade_solo", 40.0),
    SensorVentoSuperEstavel("vento_velocidade", 5.0),
    Sensor("co_ar", 4.0),
    Sensor("qualidade_ar", 8.0),
    SensorDirecaoVento("vento_direcao", 0.0)
]

def medir_memoria():
    return psutil.Process().memory_info().rss / (1024 * 1024)

def medir_memoria_maxima():
    return tracemalloc.get_traced_memory()[1] / (1024 * 1024)

def gerar_dados_paralelo(qtd_dados, num_threads=None):
    if num_threads is None:
        num_threads = multiprocessing.cpu_count()

    parte = qtd_dados // num_threads
    resto = qtd_dados % num_threads
    tamanhos_partes = [parte] * num_threads
    for i in range(resto):
        tamanhos_partes[i] += 1

    def tarefa(qtd):
        return [tuple(sensor.medir() for sensor in sensors) for _ in range(qtd)]

    dados = []
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futuros = [executor.submit(tarefa, qtd) for qtd in tamanhos_partes]
        for futuro in as_completed(futuros):
            dados.extend(futuro.result())

    return dados

def data_to_json(data):
    json_string = []
    for i in range(len(data[0])):
        json_string.append({
            "temperatura": data[0][i],
            "airHumidity": data[1][i],
            "airSoil": data[2][i],
            "co2": data[4][i],
            "airQuality": data[5][i],
            "windSpeed": data[3][i],
            "windDirection": data[6][i],
            "insertDate": datetime.datetime.now().isoformat()
        })

    return json.dumps(json_string)

def iniciar_teste():
    tracemalloc.start()
    ranges = [10, 100, 1000]
    # 10000, 100000, 500000, 500000, 500000]
    desempenho = []

    for qtd_dados in ranges:
        print(f"\n🔄 Gerando {qtd_dados} dados com threads...")
        inicio = time.time()
        data = gerar_dados_paralelo(qtd_dados)
        data = data_to_json(data)
        fim_total = time.time()

        file = f"{datetime.datetime.now().isoformat()}.json"

        with open(file, "w") as f:
            f.write(data)

        s3.upload_file(file, s3_name, f"data/{file}")

        if os.path.exists(file):
            os.remove(file)
            print("Arquivo apagado com sucesso!")


        mem_usada = medir_memoria()
        mem_max = medir_memoria_maxima()
        tempo_execucao = fim_total - inicio
        desempenho.append((qtd_dados, tempo_execucao, mem_usada, mem_max))
        print(f"✅ {qtd_dados} dados processados em {tempo_execucao:.2f}s, Memória: {mem_usada:.2f} MB, Máxima: {mem_max:.2f} MB")


if __name__ == "__main__":
    iniciar_teste()
