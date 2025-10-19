import boto3
import pandas as pd
import psycopg2
from io import StringIO
from datetime import datetime

# ---------------- CONFIGURAÇÕES ----------------
DB_HOST = "eco-fire-watch-client.co6mdrjhs9iz.us-east-1.rds.amazonaws.com"
DB_NAME = "postgres"
DB_USER = "efwUserDatabase"
DB_PASSWORD = "groupoEfw2025"
DB_PORT = 5432

# Valor fixo para o campo "location"
LOCATION_PADRAO = "São Paulo, São Paulo"

# ---------------- FUNÇÃO PRINCIPAL ----------------
def main():
    # --- Conectar ao S3 ---
    s3 = boto3.Session(profile_name="default").client("s3")

    # --- Buscar o arquivo mais recente ---
    response = s3.list_objects_v2(Bucket="eco-fire-watch-test-trusted", Prefix="inmet/")
    if "Contents" not in response:
        print("Nenhum arquivo encontrado no bucket.")
        return

    latest_file = max(response["Contents"], key=lambda x: x["LastModified"])
    latest_key = latest_file["Key"]
    print(f"Último arquivo encontrado: {latest_key}")

    # --- Baixar o CSV ---
    csv_obj = s3.get_object(Bucket="eco-fire-watch-test-trusted", Key=latest_key)
    csv_content = csv_obj["Body"].read().decode("utf-8")

    # --- Ler CSV ---
    df = pd.read_csv(StringIO(csv_content), sep=";")

    # --- Tratar e converter os campos necessários ---
    # Combina DATA e HORA_UTC para gerar o timestamp
    df["timestamp"] = df["DATA"].astype(str) + " " + df["HORA_UTC"].astype(str)
    df["timestamp"] = pd.to_datetime(df["timestamp"], format="%Y-%m-%d %H:%M:%S")

    # Seleciona apenas as colunas relevantes
    df_final = df[[
        "timestamp",
        "TEMPERATURA_DO_AR_BULBO_SECO_HORARIA_DEGC",
        "UMIDADE_RELATIVA_DO_AR_HORARIA"
    ]].copy()

    # Renomeia para os nomes do banco
    df_final.columns = ["timestamp", "temperature", "humidity"]

    # Adiciona a coluna "location" com valor fixo
    df_final["location"] = LOCATION_PADRAO

    # --- Conectar ao PostgreSQL ---
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            port=DB_PORT
        )
        cur = conn.cursor()
        print("Conexão ao banco de dados bem-sucedida.")

        # --- Inserir os dados ---
        for _, row in df_final.iterrows():
            try:
                cur.execute("""
                    INSERT INTO inmet_history (timestamp, temperature, humidity, location)
                    VALUES (%s, %s, %s, %s)
                """, (
                    row["timestamp"],
                    float(row["temperature"]) if not pd.isna(row["temperature"]) else None,
                    float(row["humidity"]) if not pd.isna(row["humidity"]) else None,
                    row["location"]
                ))
            except Exception as e:
                print(f"Erro ao inserir linha: {e}")

        conn.commit()
        print("Inserção concluída com sucesso!")

    except Exception as e:
        print(f"Erro ao conectar ao banco: {e}")

    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()
        print("Conexão encerrada.")


if __name__ == "__main__":
    main()
