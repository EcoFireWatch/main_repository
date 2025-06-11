import time
import boto3
import gspread
import csv
import io
from google.oauth2.service_account import Credentials

AWS_ACCESS_KEY_ID = ''
AWS_SECRET_ACCESS_KEY = ''
AWS_SESSION_TOKEN = ''

BUCKET_NAME = 'ecofirewatch-trusted'
PREFIX = ''
POLL_INTERVAL = 60

SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
SERVICE_ACCOUNT_FILE = r'ecofirewatch-0089310b8142.json'
SPREADSHEET_ID = '1TLGOryLr4zrdEzJy9QgioeYZePlxJr9xr8vQKJAHXdo'
WORKSHEET_NAME = 'Sensor'

credentials = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
gc = gspread.authorize(credentials)
sheet = gc.open_by_key(SPREADSHEET_ID).worksheet(WORKSHEET_NAME)

s3 = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    aws_session_token=AWS_SESSION_TOKEN
)

def listar_ultimo_csv_s3():
    response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=PREFIX)
    if 'Contents' in response:
        arquivos_csv = [obj for obj in response['Contents'] if obj['Key'].endswith('.csv')]
        if not arquivos_csv:
            return None
        ultimo_arquivo = sorted(arquivos_csv, key=lambda x: x['LastModified'], reverse=True)[0]
        return ultimo_arquivo['Key']
    return None

def baixar_arquivo_s3(key):
    response = s3.get_object(Bucket=BUCKET_NAME, Key=key)
    return response['Body'].read().decode('utf-8')

def processar_csv_e_enviar_para_sheets(conteudo_arquivo):
    try:
        reader = csv.reader(io.StringIO(conteudo_arquivo), delimiter=';')
        linhas = list(reader)

        if not linhas:
            print("CSV vazio.")
            return

        sheet.clear()
        sheet.update('A1', linhas)
        print("Dados CSV enviados com sucesso ao Google Sheets.")

    except Exception as e:
        print(f"Erro ao processar e enviar CSV para o Sheets: {e}")

def main():
    ultimo_arquivo_visto = None
    while True:
        try:
            ultimo = listar_ultimo_csv_s3()
            if ultimo and ultimo != ultimo_arquivo_visto:
                print(f'Novo arquivo detectado: {ultimo}')
                conteudo = baixar_arquivo_s3(ultimo)
                processar_csv_e_enviar_para_sheets(conteudo)
                ultimo_arquivo_visto = ultimo
            else:
                print("Nenhum novo arquivo CSV encontrado.")

            time.sleep(POLL_INTERVAL)

        except Exception as e:
            print(f'Erro: {e}')
            time.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    main()
