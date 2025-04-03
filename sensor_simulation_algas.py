import random
import time
import tracemalloc
import psutil
import mysql.connector
import threading
import queue
import concurrent.futures
import matplotlib.pyplot as plt

def conectar_bd():
    return mysql.connector.connect(
        host="127.0.0.1",
        user="Aluno",
        password="urubu100",
        database="eco_fire_watch"
    )

def criar_tabela():
    conn = conectar_bd()
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS sensores (
            id INT AUTO_INCREMENT PRIMARY KEY,
            temperatura FLOAT,
            umidade_ar FLOAT,
            umidade_solo FLOAT,
            vento_velocidade FLOAT,
            co_ar FLOAT,
            qualidade_ar FLOAT,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    conn.commit()
    conn.close()

criar_tabela()

class Sensor:
    def __init__(self, nome, valor_inicial):
        self.nome = nome
        self.valor = valor_inicial
    
    def medir(self):
        """Gera medições aleatórias dentro da faixa esperada."""
        self.valor = round(self.valor + random.uniform(-0.5, 0.5), 2)
        return self.valor

sensors = [
    Sensor("temperatura", 25.0),
    Sensor("umidade_ar", 60.0),
    Sensor("umidade_solo", 40.0),
    Sensor("vento_velocidade", 5.0),
    Sensor("co_ar", 4.0),
    Sensor("qualidade_ar", 8.0)
]

def medir_memoria():
    return psutil.Process().memory_info().rss / (1024 * 1024)

def medir_memoria_maxima():
    return tracemalloc.get_traced_memory()[1] / (1024 * 1024)

def gerar_dados(qtd_dados):
    """Gera dados usando threads para melhorar a performance"""
    def worker(q, qtd):
        """Thread worker para gerar medições"""
        for _ in range(qtd):
            leitura = tuple(sensor.medir() for sensor in sensors)
            q.put(leitura)
    
    q = queue.Queue()
    num_threads = min(10, qtd_dados // 1000 + 1)
    threads = []

    for _ in range(num_threads):
        t = threading.Thread(target=worker, args=(q, qtd_dados // num_threads))
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    return [q.get() for _ in range(q.qsize())]

def salvar_no_banco(dados):
    """Salva os dados no banco em lotes otimizados"""
    conn = conectar_bd()
    cursor = conn.cursor()
    
    batch_size = 500000
    for i in range(0, len(dados), batch_size):
        cursor.executemany("""
            INSERT INTO sensores (temperatura, umidade_ar, umidade_solo, vento_velocidade, co_ar, qualidade_ar)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, dados[i:i + batch_size])
        conn.commit()

    conn.close()

def gerar_graficos(desempenho):
    """Gera gráficos do desempenho da geração e salvamento de dados."""
    qtd_dados, tempos_geracao, tempos_salvamento, mem_usada, mem_max = zip(*desempenho)
    
    plt.figure(figsize=(12, 6))
    
    plt.subplot(1, 3, 1)
    plt.plot(qtd_dados, tempos_geracao, marker='o', label="Tempo de Geração")
    plt.plot(qtd_dados, tempos_salvamento, marker='s', linestyle='dashed', color='red', label="Tempo de Salvamento")
    plt.xlabel("Quantidade de Dados")
    plt.ylabel("Tempo (s)")
    plt.title("Desempenho da Geração e Salvamento")
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
    plt.show()

def iniciar_teste():
    tracemalloc.start()
    ranges = [
        10,
        100,
        1000,
        10000,
        100000,
        1000000,
        3000000
    ]
    desempenho = []
    
    for qtd_dados in ranges:
        print(f"\n🔄 Gerando {qtd_dados} dados...")

        inicio_geracao = time.time()
        dados = gerar_dados(qtd_dados)
        fim_geracao = time.time()
        
        print(f"💾 Salvando {qtd_dados} dados no banco...")
        
        inicio_salvamento = time.time()
        salvar_no_banco(dados)
        fim_salvamento = time.time()

        mem_usada = medir_memoria()
        mem_max = medir_memoria_maxima()
        tempo_geracao = fim_geracao - inicio_geracao
        tempo_salvamento = fim_salvamento - inicio_salvamento

        desempenho.append((qtd_dados, tempo_geracao, tempo_salvamento, mem_usada, mem_max))

        print(f"✅ {qtd_dados} dados gerados em {tempo_geracao:.2f}s, salvos em {tempo_salvamento:.2f}s, Memória: {mem_usada:.2f} MB, Máxima: {mem_max:.2f} MB")

    gerar_graficos(desempenho)

if __name__ == "__main__":
    iniciar_teste()
