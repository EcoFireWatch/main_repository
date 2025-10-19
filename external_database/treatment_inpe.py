import pandas as pd
from unidecode import unidecode
import numpy as np
import re
import base64
import io

def puxar_arquivo_e_trasformar_base64(caminho_arquivo):
    with open(caminho_arquivo, "rb") as file:
        encoded_string = base64.b64encode(file.read()).decode('utf-8')
    return encoded_string

csv_base64 = puxar_arquivo_e_trasformar_base64("focos_diario_br_20251009.csv")
csv_bytes = base64.b64decode(csv_base64)
csv_io = io.BytesIO(csv_bytes)

df = pd.read_csv(
    csv_io,
    sep=',',
    encoding='latin-1'
)

def padroniza_nome(col):
    col = unidecode(col).upper()
    col = re.sub(r'[^A-Z0-9_]', '_', col)
    col = re.sub(r'_+', '_', col)
    col = col.strip('_')
    return col

df.columns = [padroniza_nome(col) for col in df.columns]
df = df.dropna(axis=1, how='all')

if 'DATA_HORA_GMT' in df.columns:
    df['DATA_HORA_GMT'] = df['DATA_HORA_GMT'].astype(str).str.strip()
    df['DATA'] = pd.to_datetime(df['DATA_HORA_GMT'], dayfirst=True, errors='coerce').dt.strftime('%Y-%m-%d')
    df['HORA_GMT'] = pd.to_datetime(df['DATA_HORA_GMT'], dayfirst=True, errors='coerce').dt.strftime('%H:%M:%S')
    df = df[df['DATA'].notna()]

colunas_numericas = df.select_dtypes(include=['float64', 'int64']).columns.tolist()
possiveis_numericas = [col for col in df.select_dtypes(include='object').columns if col not in ['data_hora_gmt']]

for col in possiveis_numericas:
    df[col] = df[col].astype(str).str.replace(',', '.').str.replace(' ', '').replace('', np.nan)
    try: 
        df[col] = df[col].astype(float)
        colunas_numericas.append(col)
    except:
        pass

colunas_numericas = list(set(colunas_numericas))
colunas_categoricas = [col for col in df.columns if col not in colunas_numericas]

df.replace(['', ' '], np.nan, inplace=True)
for col in colunas_numericas:
    df[col].fillna(df[col].mean(), inplace=True)

df.drop_duplicates(inplace=True)

for col in colunas_categoricas:
    if df[col].dtype == object:
        df[col] = df[col].astype(str).apply(lambda x: unidecode(x).upper().replace("'", "").replace("`", ""))

if 'BIOMA' in df.columns:
    df['BIOMA'] = df['BIOMA'].replace({
        'AMAZANIA': 'AMAZONIA',
        'MATAATLAC/NTICA': 'MATA ATLANTICA'
    })

colunas_finais = [
    'ID', 'LAT', 'LON', 'DATA_HORA_GMT', 'SATELITE',
    'MUNICIPIO', 'ESTADO', 'BIOMA', 'FRP'
]

df = df[colunas_finais]
colunas_finais = [
    'ID', 'LAT', 'LON', 'DATA_HORA_GMT', 'SATELITE',
    'MUNICIPIO', 'ESTADO', 'BIOMA', 'FRP'
]

df = df[[col for col in colunas_finais if col in df.columns]]

df.to_csv('focos_diario_br_20251009_TRUSTED.csv', index=False, sep=';')
print("Arquivo salvo como focos_diario_br_20251009_TRUSTED.csv")