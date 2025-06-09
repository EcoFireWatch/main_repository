from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

import time
import getpass
import json
import re
import emoji
import csv
import datetime
import boto3

# AWS CONFIGURAÇÕES
AWS_ACCESS_KEY_ID = "leandro"
AWS_SECRET_ACCESS_KEY = "leandro"
AWS_SESSION_TOKEN = "leandro"
BUCKET_NAME = "leandro"
S3_KEY = "blacklist_trusted.csv"

# CONFIGURAÇÕES SELENIUM
chrome_options = Options()
chrome_options.add_argument("--start-maximized")
chrome_options.add_argument("--headless=new")
chrome_options.add_argument('--no-sandbox')
chrome_options.add_argument('--disable-dev-shm-usage')

service = Service(executable_path='/home/ubuntu/chromedriver-linux64/chromedriver')
driver = webdriver.Chrome(service=service, options=chrome_options)

# LISTA DE LINKS
links_especificos = [
    "https://x.com/NewsLiberdade/status/1835049315011789051",
    "https://x.com/GugaNoblat/status/1827791747138679157",
    "https://x.com/Metropoles/status/1827065848684802224",
    "https://x.com/whindersson/status/1437921972286017545",
    "https://x.com/luansantana/status/1305976903787048962"
]

# PALAVRÕES
palavroes = {
    "idiota", "bobão", "babaca", "burro", "tonto", "estúpido", "imbecil", "cretino",
    "otário", "palhaço", "besta", "tolo", "trouxa", "pateta", "bobalhão",
    "cretina", "vagabundo", "piranha", "viado", "puta", "merda", "caralho",
    "fudeu", "fuder", "safado", "desgraçado", "caceta", "fudido", "caralhudo", "porra", "buceta",
    "bucetuda", "bosta", "cu", "cuzão", "boceta", "bocetuda", "maldito", "maldita", "arrombado", "foda"
}

# FUNÇÕES
def login(driver, username, password):
    username_input = WebDriverWait(driver, 20).until(
        EC.presence_of_element_located((By.NAME, "text"))
    )
    username_input.send_keys(username)
    username_input.send_keys(Keys.RETURN)
    password_input = WebDriverWait(driver, 15).until(
        EC.presence_of_element_located((By.NAME, "password"))
    )
    password_input.send_keys(password)
    password_input.send_keys(Keys.RETURN)

def scroll_and_collect(driver, scrolls):
    tweets_data = []
    for _ in range(scrolls):
        driver.find_element(By.TAG_NAME, 'body').send_keys(Keys.END)
        time.sleep(2)
        tweet_elements = driver.find_elements(By.XPATH, '//div[@data-testid="tweetText"]')
        for tweet_element in tweet_elements:
            texto = tweet_element.text
            try:
                time_element = tweet_element.find_element(By.XPATH, './/ancestor::article//time')
                data = time_element.get_attribute("datetime")
            except:
                data = None
            tweets_data.append({"tweet": texto, "data": data})
    return tweets_data

def collect_replies_from_links(driver, links, scrolls_per_tweet=5):
    replies_data = []
    for link in links:
        driver.get(link)
        time.sleep(4)
        for _ in range(scrolls_per_tweet):
            driver.find_element(By.TAG_NAME, 'body').send_keys(Keys.END)
            time.sleep(2)
        reply_elements = driver.find_elements(By.XPATH, '//div[@data-testid="tweetText"]')
        for reply_element in reply_elements:
            texto = reply_element.text
            try:
                time_element = reply_element.find_element(By.XPATH, './/ancestor::article//time')
                data = time_element.get_attribute("datetime")
            except:
                data = None
            replies_data.append({"tweet": texto, "data": data})
    return replies_data

def levenshtein(p1, p2):
    p1_len, p2_len = len(p1), len(p2)
    matrix = [[0] * (p2_len + 1) for _ in range(p1_len + 1)]
    for i in range(p1_len + 1): matrix[i][0] = i
    for j in range(p2_len + 1): matrix[0][j] = j
    for i in range(1, p1_len + 1):
        for j in range(1, p2_len + 1):
            cost = 0 if p1[i - 1] == p2[j - 1] else 1
            matrix[i][j] = min(matrix[i - 1][j] + 1,
                               matrix[i][j - 1] + 1,
                               matrix[i - 1][j - 1] + cost)
    return matrix[p1_len][p2_len]

def contem_palavrao(tweet, palavroes):
    palavras = tweet.lower().split()
    for palavra in palavras:
        for palavrao in palavroes:
            max_len = max(len(palavra), len(palavrao))
            if max_len == 0:
                continue
            distancia = levenshtein(palavra, palavrao)
            similaridade = 1 - (distancia / max_len)
            if similaridade > 0.9:
                return True
    return False

def limpar_tweet(tweet_text):
    tweet_text = re.sub(r"http\S+", "", tweet_text)
    tweet_text = re.sub(r"@\w+", "", tweet_text)
    tweet_text = re.sub(r"[^\w\s]", "", tweet_text)
    tweet_text = re.sub(r"\n", " ", tweet_text)
    tweet_text = emoji.replace_emoji(tweet_text, replace="")
    tweet_text = tweet_text.lower().strip()
    if contem_palavrao(tweet_text, palavroes):
        stats["palavroesRemovidos"] += 1
        return None
    return tweet_text

def upload_file_to_s3(local_file_path, bucket_name, s3_key):
    session = boto3.Session(
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        aws_session_token=AWS_SESSION_TOKEN
    )
    s3 = session.client('s3')
    try:
        s3.upload_file(local_file_path, bucket_name, s3_key)
        print(f"Arquivo enviado para s3://{bucket_name}/{s3_key}")
    except Exception as e:
        print(f"Erro ao enviar arquivo para S3: {e}")

# EXECUÇÃO
stats = {"palavroesRemovidos": 0}
months = {i: {"name": name, "count": 0} for i, name in enumerate(
    ["Janeiro", "Fevereiro", "Março", "Abril", "Maio", "Junho",
     "Julho", "Agosto", "Setembro", "Outubro", "Novembro", "Dezembro"], start=1)}

try:
    # WEB SCRAPING
    start_time = time.time()
    driver.get('https://x.com/search?q=monitoramento%20queimadas&src=typed_query&f=live')

    username = input("Digite seu usuário do X: ")
    password = getpass.getpass("Digite sua senha do X: ")
    login(driver, username, password)
    time.sleep(5)

    tweets_live = scroll_and_collect(driver, 40)
    print(f"{len(tweets_live)} tweets live carregados.")

    driver.get('https://x.com/search?q=queimadas&src=typed_query&f=top')
    tweets_top = scroll_and_collect(driver, 20)
    print(f"{len(tweets_top)} tweets top carregados.")

    replies = collect_replies_from_links(driver, links_especificos)
    print(f"{len(replies)} respostas coletadas.")

    tweets_data = tweets_live + tweets_top + replies

    # FILTRAGEM E LIMPEZA
    tweets_filtrados = []
    tweets_vistos = set()

    for tweet_obj in tweets_data:
        texto_original = tweet_obj["tweet"]
        tweet_limpo = limpar_tweet(texto_original)

        if tweet_limpo:
            if tweet_limpo not in tweets_vistos:
                tweets_filtrados.append({
                    "tweet": tweet_limpo,
                    "data": tweet_obj["data"]
                })
                tweets_vistos.add(tweet_limpo)
        else:
            if tweet_obj["data"]:
                date = datetime.datetime.strptime(tweet_obj["data"], "%Y-%m-%dT%H:%M:%S.%fZ")
                months[date.month]["count"] += 1

    # SALVA JSON
    with open("tweets_queimadas_trusted.json", "w", encoding="utf-8") as f:
        json.dump(tweets_filtrados, f, ensure_ascii=False, indent=4)

    # SALVA CSV
    csv_filename = "blacklist_trusted.csv"
    with open(csv_filename, "w", encoding="utf-8", newline='') as f:
        writer = csv.writer(f, quoting=csv.QUOTE_ALL)
        writer.writerow(["mes", "quantidade"])
        for month in months.values():
            writer.writerow([month["name"], month["count"]])

    # ENVIA PARA S3
    upload_file_to_s3(local_file_path=csv_filename, bucket_name=BUCKET_NAME, s3_key=S3_KEY)

    print(f"Número de palavrões removidos: {stats['palavroesRemovidos']}")
    print(f"Tempo total de execução: {time.time() - start_time:.2f} segundos")

finally:
    driver.quit()
