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

# ---------------- FUNÇÃO PRINCIPAL ----------------
def main():
    # --- Conectar ao S3 ---
    s3 = boto3.Session(profile_name="default").client("s3")

    # --- Pegar o arquivo mais recente ---
    response = s3.list_objects_v2(Bucket="eco-fire-watch-test-trusted", Prefix="inpe/")
    if "Contents" not in response:
        print("Nenhum arquivo encontrado no bucket.")
        return

    latest_file = max(response["Contents"], key=lambda x: x["LastModified"])
    latest_key = latest_file["Key"]
    print(f"Último arquivo encontrado: {latest_key}")

    # --- Baixar o arquivo ---
    csv_obj = s3.get_object(Bucket="eco-fire-watch-test-trusted", Key=latest_key)
    csv_content = csv_obj["Body"].read().decode("utf-8")

    # --- Ler CSV ---
    df = pd.read_csv(StringIO(csv_content), sep=";")

    # --- Conectar ao PostgreSQL ---
    try:
        conn = psycopg2.connect(
            host="eco-fire-watch-client.co6mdrjhs9iz.us-east-1.rds.amazonaws.com",
            database="postgres",
            user="efwUserDatabase",
            password="groupoEfw2025",
            port=5432
        )
        cur = conn.cursor()
        print("Conexão ao banco de dados bem-sucedida.")

        # --- Inserir dados ---
        for _, row in df.iterrows():
            try:
                cur.execute("""
                    INSERT INTO inpe_fire (latitude, longitude, timestamp, municipality, state, biome, frp)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (
                    float(row["LAT"]),
                    float(row["LON"]),
                    datetime.strptime(row["DATA_HORA_GMT"], "%Y-%m-%d%H:%M:%S"),
                    row["MUNICIPIO"],
                    row["ESTADO"],
                    row["BIOMA"],
                    float(row["FRP"])
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
