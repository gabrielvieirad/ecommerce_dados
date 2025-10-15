import pandas as pd

CAMINHO = r'dados/csv_marketplaces/mercadolivre.xlsx'

print("[INFO] Lendo planilha para teste...")
df = pd.read_excel(CAMINHO, dtype=str)

# Mostrar colunas disponíveis
print("\n[INFO] Colunas encontradas:")
print(df.columns.tolist())

# Mostrar amostra das primeiras 10 linhas da coluna 'Data da venda'
possiveis_colunas = [c for c in df.columns if 'data' in c.lower()]
if not possiveis_colunas:
    print("\n[ERRO] Nenhuma coluna com 'data' encontrada.")
else:
    col_data = possiveis_colunas[0]
    print(f"\n[INFO] Primeira coluna de data identificada: {col_data}")
    print("\n[AMOSTRA] Primeiras 10 datas lidas:")
    print(df[col_data].head(10).to_list())

print("[DEBUG] Amostras de data convertidas:")
print(df['data'].head(10).to_list())
