import time
import boto3
import gspread
from google.oauth2.service_account import Credentials
import csv
from io import StringIO

AWS_ACCESS_KEY_ID = 'leandro'
AWS_SECRET_ACCESS_KEY = 'leandro'
AWS_SESSION_TOKEN = 'leandro'

BUCKET_NAME = 'leandro-tokudome'
PREFIX = ''
POLL_INTERVAL = 60

SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
SERVICE_ACCOUNT_FILE = r'C:\Users\akiot\OneDrive\Área de Trabalho\Pasta\SpTech\aulaCesar\ecofirewatch-0089310b8142.json'
SPREADSHEET_ID = '1TLGOryLr4zrdEzJy9QgioeYZePlxJr9xr8vQKJAHXdo'
WORKSHEET_NAME = 'Webscrapping'

credentials = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
gc = gspread.authorize(credentials)
sheet = gc.open_by_key(SPREADSHEET_ID).worksheet(WORKSHEET_NAME)

s3 = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    aws_session_token=AWS_SESSION_TOKEN
)

def listar_arquivos_s3():
    response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=PREFIX)
    if 'Contents' in response:
        return [obj['Key'] for obj in response['Contents']]
    return []

def baixar_arquivo_s3(key):
    response = s3.get_object(Bucket=BUCKET_NAME, Key=key)
    return response['Body'].read().decode('utf-8')

def processar_e_enviar_para_sheets(conteudo_arquivo):
    f = StringIO(conteudo_arquivo)
    leitor = csv.reader(f)
    linhas = list(leitor)
    
    sheet.clear()
    
    sheet.update('A1', linhas)
    
    print("Dados substituídos no Google Sheets com sucesso.")


def main():
    arquivos_vistos = set()
    while True:
        try:
            arquivos = listar_arquivos_s3()
            novos = [a for a in arquivos if a not in arquivos_vistos]

            for arquivo in novos:
                print(f'Novo arquivo detectado: {arquivo}')
                conteudo = baixar_arquivo_s3(arquivo)
                processar_e_enviar_para_sheets(conteudo)
                arquivos_vistos.add(arquivo)

            time.sleep(POLL_INTERVAL)
        except Exception as e:
            print(f'Erro: {e}')
            time.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    main()
