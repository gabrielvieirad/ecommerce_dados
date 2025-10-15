import pandas as pd
import re
from datetime import datetime

# ======== 1. SHOPEE =========
def padronizar_shopee(caminho_csv):
    df = pd.read_csv(caminho_csv, dtype=str)

    # Normalizar nomes das colunas
    df.columns = df.columns.str.strip().str.lower()

    # Procurar coluna de SKU (pode ter variação)
    col_sku = next((c for c in df.columns if 'sku' in c), None)
    col_valor = next((c for c in df.columns if 'valor' in c), None)

    if col_sku:
        df[col_sku] = df[col_sku].astype(str).str.replace(r'\.0$', '', regex=True)
    else:
        print("⚠️ Nenhuma coluna SKU encontrada no Shopee.")

    if col_valor:
        df[col_valor] = pd.to_numeric(df[col_valor], errors='coerce').round(2)
    else:
        print("⚠️ Nenhuma coluna de valor encontrada no Shopee.")

    df.to_csv('shopee_padronizado.csv', index=False)
    print("✅ Shopee padronizado salvo como 'shopee_padronizado.csv'.")



# ======== 2. BELEZA NA WEB =========
def padronizar_blz(caminho_xlsx):
    df = pd.read_excel(caminho_xlsx, dtype=str)

    # Padronizar formato da data
    if 'data' in df.columns:
        df['data'] = pd.to_datetime(df['data'], errors='coerce').dt.strftime('%d/%m/%Y')

    # Remover * do SKU
    if 'sku' in df.columns:
        df['sku'] = df['sku'].astype(str).str.replace('*', '', regex=False).str.strip()

    # Salvar resultado
    df.to_excel('blz_padronizado.xlsx', index=False)
    print("Beleza na Web padronizado salvo como 'blz_padronizado.xlsx'.")


# ======== 3. MERCADO LIVRE =========
def padronizar_mercadolivre(caminho_csv):
    df = pd.read_csv(caminho_csv, dtype=str)

    # Remover data do nome do produto (ex: "Produto XYZ - 12/10/2025" -> "Produto XYZ")
    if 'produto' in df.columns:
        df['produto'] = df['produto'].str.replace(r'\s*[-–]\s*\d{1,2}/\d{1,2}/\d{2,4}', '', regex=True)

    # Remover células vazias
    df = df.dropna(how='all')

    # Salvar resultado
    df.to_csv('mercadolivre_padronizado.csv', index=False)
    print("Mercado Livre padronizado salvo como 'mercadolivre_padronizado.csv'.")


# ======== EXECUÇÃO =========
if __name__ == "__main__":
    padronizar_shopee(r"C:\Users\new big\Desktop\projeto_ecommerce_dados\dados\csv_marketplaces\shopee_merged.csv")
    padronizar_blz(r"C:\Users\new big\Desktop\projeto_ecommerce_dados\dados\csv_marketplaces\blz.xlsx")
    padronizar_mercadolivre(r"C:\Users\new big\Desktop\projeto_ecommerce_dados\dados\csv_marketplaces\mercadolivre_agrupado.csv")


    print("\n Padronização concluída com sucesso!")
