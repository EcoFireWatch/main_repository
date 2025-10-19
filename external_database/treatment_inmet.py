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

csv_base64 = puxar_arquivo_e_trasformar_base64("INMET_SE_SP_A771_SAO PAULO - INTERLAGOS_01-01-2025_A_30-09-2025.CSV")

csv_bytes = base64.b64decode(csv_base64)

csv_io = io.BytesIO(csv_bytes)
df = pd.read_csv(
    csv_io,
    skiprows=8,
    sep=';',
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

df['DATA'] = df['DATA'].astype(str).str.strip()
df['DATA'] = pd.to_datetime(df['DATA'], dayfirst=True, errors='coerce')
df = df[df['DATA'].notna()]
df['DATA'] = df['DATA'].dt.strftime('%Y-%m-%d')

if 'HORA_UTC' in df.columns:
    def hora_formatada(x):
        if pd.isnull(x):
            return 'AUSENTE'
        x_str = str(x).strip()
        match = re.match(r'^(\d{2})', x_str)
        if match:
            return f"{match.group(1)}:00:00"
        return 'AUSENTE'
    df['HORA_UTC'] = df['HORA_UTC'].apply(hora_formatada)

colunas_numericas = df.select_dtypes(include=['float64', 'int64']).columns.tolist()
possiveis_numericas = [col for col in df.select_dtypes(include='object').columns if col not in ['DATA', 'HORA_UTC']]

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
        df[col] = df[col].astype(str).apply(lambda x: unidecode(x).upper())

df.to_csv('INMET_SE_SP_A771_SAO_PAULO_INTERLAGOS_2025_TRUSTED.csv', index=False, sep=';')
print("Arquivo salvo como INMET_SE_SP_A771_SAO_PAULO_INTERLAGOS_2025_TRUSTED.csv")