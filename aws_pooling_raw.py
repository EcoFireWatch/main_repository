import boto3
import json
import csv
import io

AWS_ACCESS_KEY_ID = ''
AWS_SECRET_ACCESS_KEY = ''
AWS_SESSION_TOKEN = ''

SOURCE_BUCKET = 'ecofirewatch-raw'
TARGET_BUCKET = 'ecofirewatch-trusted'
PREFIX = ''
EXTENSAO_JSON = '.json'

s3 = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    aws_session_token=AWS_SESSION_TOKEN
)

def listar_arquivos_json():
    response = s3.list_objects_v2(Bucket=SOURCE_BUCKET, Prefix=PREFIX)
    if 'Contents' in response:
        return [obj['Key'] for obj in response['Contents'] if obj['Key'].endswith(EXTENSAO_JSON)]
    return []

def baixar_arquivo_s3(bucket, key):
    response = s3.get_object(Bucket=bucket, Key=key)
    return response['Body'].read().decode('utf-8')

def jsonl_para_csv(jsonl_str):
    # São os campos que o Stream Analytics coloca
    campos_removidos = {
        'EventProcessedUtcTime',
        'PartitionId',
        'EventEnqueuedUtcTime',
        'IoTHub'
    }

    linhas_json = jsonl_str.strip().split('\n')
    dados = []

    for linha in linhas_json:
        if linha.strip():
            obj = json.loads(linha)
            for campo in campos_removidos:
                obj.pop(campo, None)
            dados.append(obj)

    if not dados:
        return None

    headers = list(dados[0].keys())

    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=headers, delimiter=";")
    writer.writeheader()
    writer.writerows(dados)

    return output.getvalue()

def pegar_ultimo_arquivo_jsonl():
    response = s3.list_objects_v2(Bucket=SOURCE_BUCKET, Prefix=PREFIX)
    if 'Contents' not in response:
        return None

    arquivos_jsonl = [
        obj for obj in response['Contents'] if obj['Key'].endswith(EXTENSAO_JSON)
    ]

    if not arquivos_jsonl:
        return None

    ultimo_arquivo = max(arquivos_jsonl, key=lambda x: x['LastModified'])
    return ultimo_arquivo['Key']

def salvar_csv_no_s3(conteudo_csv, nome_arquivo_csv):
    s3.put_object(
        Bucket=TARGET_BUCKET,
        Key=nome_arquivo_csv,
        Body=conteudo_csv.encode('utf-8'),
        ContentType='text/csv'
    )
    print(f"Arquivo CSV salvo em {TARGET_BUCKET}/{nome_arquivo_csv}")

def main():
    try:
        ultimo_arquivo = pegar_ultimo_arquivo_jsonl()

        if not ultimo_arquivo:
            print("Nenhum arquivo JSONL encontrado no bucket.")
            return

        print(f"Processando o último arquivo: {ultimo_arquivo}")
        conteudo = baixar_arquivo_s3(SOURCE_BUCKET, ultimo_arquivo)
        csv_str = jsonl_para_csv(conteudo)

        if csv_str:
            nome_csv = ultimo_arquivo.rsplit('.', 1)[0] + '.csv'
            salvar_csv_no_s3(csv_str, nome_csv)
        else:
            print("Nenhum dado válido encontrado.")

    except Exception as e:
        print(f"Erro: {e}")

if __name__ == '__main__':
    main()
