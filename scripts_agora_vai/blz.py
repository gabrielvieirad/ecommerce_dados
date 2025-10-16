import pandas as pd
import os

# === Caminho base ===
base_path = r"C:\Users\new big\Desktop\projeto_ecommerce_dados\dados\csv_marketplaces\padronizados"
arquivo_entrada = os.path.join(base_path, "blz_padronizado.xlsx")
arquivo_saida = os.path.join(base_path, "blz_processado.xlsx")

# === Ler o arquivo ===
df = pd.read_excel(arquivo_entrada)

# === Garantir que as colunas existam com nomes padrão ===
df.columns = [c.strip().lower() for c in df.columns]

# Detectar nomes prováveis de colunas
col_sku = next((c for c in df.columns if 'sku' in c), None)
col_desc = next((c for c in df.columns if 'desc' in c or 'produto' in c or 'nome' in c), None)
col_data = next((c for c in df.columns if 'data' in c), None)
col_vendas = next((c for c in df.columns if 'venda' in c), None)
col_valor = next((c for c in df.columns if 'valor' in c), None)

if not all([col_sku, col_desc, col_data, col_vendas, col_valor]):
    raise ValueError("Não consegui identificar automaticamente as colunas. Verifique os nomes do arquivo.")

# === Converter coluna 'data' para datetime e extrair mês e ano ===
df[col_data] = pd.to_datetime(df[col_data], format='%d/%m/%Y', errors='coerce')
df['ano'] = df[col_data].dt.year
df['mes'] = df[col_data].dt.month

# === Função para limpar valores numéricos ===
def limpar_valor(valor):
    if pd.isna(valor):
        return 0.0
    valor = str(valor).replace('R$', '').replace(' ', '').replace('\xa0', '')
    valor = ''.join(ch for ch in valor if ch.isdigit() or ch in [',', '.'])
    if valor.count('.') > 1:
        partes = valor.split('.')
        valor = ''.join(partes[:-1]) + '.' + partes[-1]
    if ',' in valor and '.' not in valor:
        valor = valor.replace(',', '.')
    try:
        return float(valor)
    except ValueError:
        return 0.0

# === Converter colunas numéricas ===
df[col_vendas] = df[col_vendas].apply(limpar_valor).astype(int)
df[col_valor] = df[col_valor].apply(limpar_valor)

# === Agrupar por SKU, Descrição, Ano, Mês ===
df_final = (
    df.groupby([col_sku, col_desc, 'ano', 'mes'], as_index=False)
      .agg({col_vendas: 'sum', col_valor: 'sum'})
)

# === Salvar o novo arquivo ===
df_final.to_excel(arquivo_saida, index=False)
print(f"Arquivo processado com sucesso em:\n{arquivo_saida}")
