import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta

# URL inicial
base_url = "https://www.cnnbrasil.com.br/tudo-sobre/queimadas/pagina/"
pagina = 1

# Data limite (1 mês atrás)
limite_tempo = datetime.now() - timedelta(days=30)

total_noticias = 0

while True:
    url = f"{base_url}{pagina}"
    print(f"🔎 Acessando: {url}")
    resp = requests.get(url, timeout=10)
    if resp.status_code != 200:
        print("❌ Página não encontrada ou erro na requisição.")
        break

    soup = BeautifulSoup(resp.text, "html.parser")
    lista = soup.select_one('ul[data-section="article_list"]')
    if not lista:
        print("⚠️ Nenhuma lista de notícias encontrada.")
        break

    noticias = lista.select("li")
    if not noticias:
        print("⚠️ Nenhuma notícia encontrada nesta página.")
        break

    count_pagina = 0

    for li in noticias:
        titulo_tag = li.select_one("h2")
        titulo = titulo_tag.get_text(strip=True) if titulo_tag else "Sem título"

        link_tag = li.select_one("a[href]")
        link = link_tag["href"] if link_tag else None

        time_tag = li.select_one("time[datetime]")
        data_publicacao = None
        if time_tag:
            try:
                data_publicacao = datetime.strptime(
                    time_tag["datetime"], "%Y-%m-%d %H:%M:%S"
                )
            except ValueError:
                # Em caso de formato diferente
                print(f"Erro ao converter data: {time_tag['datetime']}")

        # Filtra notícias dentro do último mês
        if data_publicacao and data_publicacao >= limite_tempo:
            count_pagina += 1
            print(f"📰 {titulo}")
            print(f"📅 {data_publicacao.strftime('%d/%m/%Y %H:%M')}")
            print(f"🔗 {link}\n")

    if count_pagina == 0:
        print("📉 Nenhuma notícia recente nesta página, encerrando busca.")
        break

    total_noticias += count_pagina
    pagina += 1

print(f"\n✅ Total de notícias no último mês: {total_noticias}")
