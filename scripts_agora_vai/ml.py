import pandas as pd
import os

# === Caminhos ===
base_path = r"C:\Users\new big\Desktop\projeto_ecommerce_dados\dados\csv_marketplaces\padronizados"
arquivo_entrada = os.path.join(base_path, "mercadolivre_padronizado.csv")
arquivo_saida = os.path.join(base_path, "mercadolivre_processado.csv")

# === Ler arquivo corretamente ===
df = pd.read_csv(arquivo_entrada, sep=';', encoding='utf-8')

# === Padronizar nomes de colunas ===
df.columns = [c.strip().lower().replace(' ', '').replace('\ufeff', '') for c in df.columns]
print("Colunas detectadas:", df.columns.tolist())

# === Converter coluna 'data' para datetime (independente do formato) ===
df['data'] = pd.to_datetime(df['data'].astype(str).str.strip(), dayfirst=True, errors='coerce')

# === Criar colunas de mês e ano ===
df['mes'] = df['data'].dt.month
df['ano'] = df['data'].dt.year

# === Remover a coluna 'data' ===
df.drop(columns=['data'], inplace=True)

# === Corrigir SKUs removendo '.0' ===
df['sku'] = df['sku'].astype(str).str.replace('.0', '', regex=False).str.strip()

# === Garantir que as colunas existem ===
col_desc = next((c for c in df.columns if 'descricao' in c or 'produto' in c or 'nome' in c), None)
if not col_desc:
    raise ValueError(f"Coluna de descrição não encontrada. Colunas disponíveis: {df.columns.tolist()}")

# === Concatenar conforme regra ===
df['descricao'] = (
    df['sku'].astype(str) + " - " +
    df[col_desc].astype(str) + " - mes " +
    df['mes'].astype(str).str.zfill(2) + " - ano " +
    df['ano'].astype(str) + " - " +
    df['vendas'].astype(str) + " vendas - " +
    df['valor_total'].astype(str) + "reais de valor_total"
)

# === Selecionar e renomear colunas ===
df = df[['sku', 'descricao', 'ano', 'mes', 'vendas', 'valor_total']]

# === Salvar resultado final ===
df.to_csv(arquivo_saida, index=False, sep=';', encoding='utf-8-sig')
print(f"✅ Arquivo processado com sucesso em:\n{arquivo_saida}")
