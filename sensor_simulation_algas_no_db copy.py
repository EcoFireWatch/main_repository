import numpy as np
import time
import tracemalloc
import psutil
import matplotlib.pyplot as plt
from concurrent.futures import ThreadPoolExecutor, as_completed
import multiprocessing

class Sensor:
    def __init__(self, nome, valor_inicial):
        self.nome = nome
        self.valor = valor_inicial

    def medir(self):
        self.valor = round(self.valor + np.random.normal(-0.5, 0.5), 2)
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
    Sensor("qualidade_ar", 8.0)
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

def gerar_graficos(desempenho):
    qtd_dados, tempos, mem_usada, mem_max = zip(*desempenho)

    plt.figure(figsize=(12, 6))

    plt.subplot(1, 3, 1)
    plt.plot(qtd_dados, tempos, marker='o', label="Tempo de Execução")
    plt.xlabel("Quantidade de Dados")
    plt.ylabel("Tempo (s)")
    plt.title("Desempenho da Geração de Dados (Threaded)")
    plt.xscale("log")
    plt.legend()

    plt.subplot(1, 3, 2)
    plt.plot(qtd_dados, mem_usada, marker='o', label="Memória Média Utilizada")
    plt.xlabel("Quantidade de Dados")
    plt.ylabel("Memória (MB)")
    plt.title("Uso Médio de Memória")
    plt.xscale("log")
    plt.legend()

    plt.subplot(1, 3, 3)
    plt.plot(qtd_dados, mem_max, marker='s', linestyle='dashed', label="Memória Máxima Utilizada", color='red')
    plt.xlabel("Quantidade de Dados")
    plt.ylabel("Memória (MB)")
    plt.title("Pico de Memória Utilizada")
    plt.xscale("log")
    plt.legend()

    plt.tight_layout()
    plt.savefig("grafico_desempenho.png")
    plt.show()

def iniciar_teste():
    tracemalloc.start()
    ranges = [10, 100, 1000, 10000, 100000, 500000, 500000, 500000]
    desempenho = []

    for qtd_dados in ranges:
        print(f"\n🔄 Gerando {qtd_dados} dados com threads...")
        inicio = time.time()
        gerar_dados_paralelo(qtd_dados)
        fim_total = time.time()

        mem_usada = medir_memoria()
        mem_max = medir_memoria_maxima()
        tempo_execucao = fim_total - inicio
        desempenho.append((qtd_dados, tempo_execucao, mem_usada, mem_max))
        print(f"✅ {qtd_dados} dados processados em {tempo_execucao:.2f}s, Memória: {mem_usada:.2f} MB, Máxima: {mem_max:.2f} MB")

    gerar_graficos(desempenho)

if __name__ == "__main__":
    iniciar_teste()
