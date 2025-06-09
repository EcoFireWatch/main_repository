import time
import boto3
import gspread
import json
from google.oauth2.service_account import Credentials

AWS_ACCESS_KEY_ID = 'ASIA6AVMTNEKCX646OJI'
AWS_SECRET_ACCESS_KEY = 'SDZMKG4yGSQ4QwboX9syQO6U69upNZKRpUK2p/5X'
AWS_SESSION_TOKEN = 'IQoJb3JpZ2luX2VjENX//////////wEaCXVzLXdlc3QtMiJGMEQCIAKfAscsVDy2FIgupLYb3OTrGiOeB8OhcyR7bdDusZSmAiAYwwJfXG0NC46LGstZC6snWiyHwJPM+b8FNMUEWIBxfyrBAgiu//////////8BEAAaDDk2MzUwODM5ODM1NiIM9vmFlgZ5YY05M30fKpUCUJRoqIhgKlNot1Ue/7UqCZNzUuXErOQpl56DGGtaKiAYI/HeBZPOzGyE/uDTsRUmpBuODk8m8JBQ0yaHJytP0NsF7EinObgg1EJ7CHqx+NF8zMJLFm/pauLQgVMQ3bpGx81lLoHCCDuX2wSMBx8aUcYMwV45ey+kOSGIV1S7Cs1kgMq55DlOIOsYrz6SHp1PJOJJVNwBrc2JJT2bGd7ycrNMuxQ4LsOBE4w8m6FqKrrNk73nIeLy7UXoIlxLiP5SBJ3MoLEyHOlqJIMAzUAXsE+Rdr1pkakrZqEC5X6/16+MtkmrTqCSrdvEeic1WZcLZReTnf5laDjVqHug5bqK5sgXaE3vxbRIifmXKcM0qHY7ZeptBDCDmp3CBjqeAVanOMaYbj6bOeMEZzGImr8/qSj4IUTxeGaeneXtJLqk893qAHq4aOy6KAYy0h7NkY0AYVNXcb8mS9d07CEjzquLXY+m3ggAT28ErnNMNFA0GzesnwNuJ/OW1JWAOyE2CcAFAxMc6iT1ohABhPeIDQr0ov44FvOsXJ7stskt8c2STeeyIJ+Qhzw1dqwnmdOtLZHEvV3g7Ai0OtQtnhHp'

BUCKET_NAME = 'eco-fire-watch'
PREFIX = ''
POLL_INTERVAL = 60

SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
SERVICE_ACCOUNT_FILE = r'main_repository\ecofirewatch-0089310b8142.json'
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

def listar_arquivos_s3():
    response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=PREFIX)
    if 'Contents' in response:
        return [obj['Key'] for obj in response['Contents']]
    return []

def baixar_arquivo_s3(key):
    response = s3.get_object(Bucket=BUCKET_NAME, Key=key)
    return response['Body'].read().decode('utf-8')

def achatar_json(dado):
    resultado = {}
    for chave, valor in dado.items():
        if isinstance(valor, dict):
            for subchave, subvalor in valor.items():
                nova_chave = f"{chave}.{subchave}"
                resultado[nova_chave] = subvalor
        else:
            resultado[chave] = valor
    return resultado

def processar_json_e_enviar_para_sheets(conteudo_arquivo):
    try:
        linhas_json = conteudo_arquivo.strip().split('\n')
        dados_raw = [json.loads(linha) for linha in linhas_json if linha.strip()]
        
        if not dados_raw:
            print("Nenhum dado JSON encontrado.")
            return

        dados = [achatar_json(dado) for dado in dados_raw]

        headers = list(dados[0].keys())
        linhas = [[item.get(h, '') for h in headers] for item in dados]

        sheet.clear()
        sheet.update('A1', [headers] + linhas)

        print("Dados JSONL enviados com sucesso ao Google Sheets.")

    except json.JSONDecodeError as e:
        print(f"Erro ao decodificar JSON: {e}")
    except Exception as e:
        print(f"Erro ao enviar para o Sheets: {e}")

def main():
    arquivos_vistos = set()
    while True:
        try:
            arquivos = listar_arquivos_s3()
            novos = [a for a in arquivos if a not in arquivos_vistos]

            for arquivo in novos:
                print(f'Novo arquivo detectado: {arquivo}')
                conteudo = baixar_arquivo_s3(arquivo)
                processar_json_e_enviar_para_sheets(conteudo)
                arquivos_vistos.add(arquivo)

            time.sleep(POLL_INTERVAL)
        except Exception as e:
            print(f'Erro: {e}')
            time.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    main()
