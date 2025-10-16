import pandas as pd
import math
import os

# === 1. Caminho base ===
base_path = r"C:\Users\new big\Desktop\projeto_ecommerce_dados\dados\csv_marketplaces\padronizados"
arquivo_entrada = os.path.join(base_path, "amz.csv")
arquivo_saida = os.path.join(base_path, "amz_processado.csv")

# === 2. Ler o arquivo ===
df = pd.read_csv(arquivo_entrada)

# === 3. Garantir que a coluna 'vendas' seja numérica (inteiro sem decimais) ===
df['vendas'] = (
    df['vendas']
    .astype(str)
    .str.replace('.', '', regex=False)
    .str.replace(',', '.', regex=False)
    .astype(float)
    .astype(int)
)

# === 4. Calcular divisões proporcionais ===
total_linhas = len(df)
partes = [4, 12, 9]  # meses em cada período
soma_partes = sum(partes)

tamanhos = [math.floor(total_linhas * p / soma_partes) for p in partes]
tamanhos[-1] = total_linhas - sum(tamanhos[:-1])  # ajuste final

# === 5. Dividir o dataframe ===
df_2023 = df.iloc[:tamanhos[0]].copy()
df_2024 = df.iloc[tamanhos[0]:tamanhos[0]+tamanhos[1]].copy()
df_2025 = df.iloc[tamanhos[0]+tamanhos[1]:].copy()

# === 6. Função para atribuir meses e anos ===
def atribuir_mes_ano(df, ano, meses):
    repeticoes = math.ceil(len(df) / len(meses))
    meses_repetidos = (meses * repeticoes)[:len(df)]
    df['ano'] = ano
    df['mes'] = meses_repetidos
    return df

df_2023 = atribuir_mes_ano(df_2023, 2023, [9,10,11,12])
df_2024 = atribuir_mes_ano(df_2024, 2024, list(range(1,13)))
df_2025 = atribuir_mes_ano(df_2025, 2025, list(range(1,10)))

# === 7. Juntar tudo novamente ===
df_final = pd.concat([df_2023, df_2024, df_2025], ignore_index=True)

# === 8. Garantir que valor_total é numérico ===
def limpar_valor(valor):
    if pd.isna(valor):
        return 0.0
    valor = str(valor)
    valor = valor.replace('R$', '').replace(' ', '').replace('\xa0', '')
    # remove tudo que não for dígito, vírgula, ponto
    valor = ''.join(ch for ch in valor if ch.isdigit() or ch in [',', '.'])
    # se tiver mais de um ponto, mantém só o último (ponto dos decimais)
    if valor.count('.') > 1:
        partes = valor.split('.')
        valor = ''.join(partes[:-1]) + '.' + partes[-1]
    # se tiver vírgula e não ponto, troca a vírgula por ponto
    if ',' in valor and '.' not in valor:
        valor = valor.replace(',', '.')
    try:
        return float(valor)
    except ValueError:
        return 0.0

df_final['valor_total'] = df_final['valor_total'].apply(limpar_valor)


# === 9. Agrupar por SKU, descrição, mês e ano ===
df_final = (
    df_final
    .groupby(['sku', 'descricao', 'ano', 'mes'], as_index=False)
    .agg({'vendas': 'sum', 'valor_total': 'sum'})
)

# === 10. Salvar o novo arquivo ===
df_final.to_csv(arquivo_saida, index=False)
print(f"Arquivo gerado com sucesso em:\n{arquivo_saida}")
