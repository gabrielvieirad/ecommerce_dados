import os
import pandas as pd
from unidecode import unidecode
import dateparser

# === CONFIGURAÇÕES ===
CAMINHO_ENTRADA = r'dados/csv_marketplaces/mercadolivre.xlsx'
CAMINHO_SAIDA = r'processados/mercadolivre_agrupado.csv'

# === FUNÇÕES AUXILIARES ===
def limpar_nome_coluna(col):
    return unidecode(col).lower().replace(" ", "").replace("(", "").replace(")", "").replace(".", "").replace("-", "").replace("/", "")

def limpar_sku(sku):
    if pd.isna(sku):
        return None
    return str(sku).strip().replace("'", "")

def converter_data_flex(texto):
    if pd.isna(texto):
        return None
    texto = str(texto).strip().lower()
    if not texto:
        return None

    # Remove "hs", "hrs", etc.
    texto = texto.replace("hs.", "").replace("hs", "").replace("hrs.", "").replace("hrs", "").strip()

    # Caso venha "08/10/2025 19:34"
    if " " in texto and "/" in texto:
        texto = texto.split()[0]

    # Tenta formato dd/mm/yyyy
    try:
        if "/" in texto:
            dia, mes, ano = texto.split("/")[:3]
            return f"{int(dia):02d}/{int(mes):02d}/{ano}"
    except Exception:
        pass

    # Tenta parsing com linguagem portuguesa
    try:
        data = dateparser.parse(texto, languages=['pt'])
        if data:
            return data.strftime('%d/%m/%Y')
    except Exception:
        pass

    return None

# === PROCESSAMENTO ===
def main():
    print("[INFO] Iniciando agrupamento do arquivo Mercado Livre...")

    if not os.path.exists(CAMINHO_ENTRADA):
        print(f"[ERRO] Arquivo não encontrado: {CAMINHO_ENTRADA}")
        return

    # === LER PLANILHA ===
    print("[INFO] Lendo planilha...")
    df = pd.read_excel(CAMINHO_ENTRADA, dtype=str)
    print(f"[OK] Planilha carregada com {len(df)} linhas e {len(df.columns)} colunas.")

    # === LIMPAR NOMES DE COLUNAS ===
    df.columns = [limpar_nome_coluna(c) for c in df.columns]

    # === MAPEAR COLUNAS ===
    mapa = {
        'datadavenda': 'data',
        'sku': 'sku',
        'titulodoanuncio': 'produto',
        'unidades': 'vendas',
        'totalbrl': 'valor_total'
    }

    colunas_encontradas = [c for c in mapa.keys() if c in df.columns]
    faltando = [c for c in mapa.keys() if c not in df.columns]

    if faltando:
        print(f"[ERRO] Colunas faltando: {faltando}")
        return

    df = df[colunas_encontradas].rename(columns=mapa)
    print(f"[OK] Colunas padronizadas: {list(df.columns)}")

    # === LIMPEZA DOS DADOS ===
    print("[INFO] Limpando dados...")

    df['data'] = df['data'].apply(converter_data_flex)
    df['sku'] = df['sku'].apply(limpar_sku)
    df['vendas'] = pd.to_numeric(df['vendas'], errors='coerce').fillna(0)
    df['valor_total'] = pd.to_numeric(df['valor_total'], errors='coerce').fillna(0.0)

    print("[DEBUG] Amostras de data convertidas:")
    print(df['data'].head(10).to_list())

    total_inicial = len(df)
    df = df.dropna(subset=['data', 'sku'])
    descartadas = total_inicial - len(df)
    print(f"[OK] Dados limpos ({len(df)} linhas válidas, {descartadas} descartadas).")

    if len(df) == 0:
        print("[ERRO] Nenhuma linha válida encontrada. Verifique o formato das datas.")
        return

    # === AGRUPAMENTO ===
    print("[INFO] Agrupando por data e SKU...")

    agrupado = (
        df.groupby(['data', 'sku'], as_index=False)
        .agg({'produto': 'first', 'vendas': 'sum', 'valor_total': 'sum'})
    )

    # === GERAR NOME CONCATENADO ===
    agrupado['produto'] = agrupado.apply(
        lambda x: f"{x['sku']} - {int(x['vendas'])}un - {x['data']}", axis=1
    )

    print(f"[OK] Agrupamento concluído ({len(agrupado)} linhas únicas).")

    # === SALVAR CSV ===
    os.makedirs(os.path.dirname(CAMINHO_SAIDA), exist_ok=True)
    agrupado.to_csv(CAMINHO_SAIDA, sep=';', index=False, encoding='utf-8')

    print(f"[OK] Arquivo final salvo em: {CAMINHO_SAIDA}")
    print("[INFO] Processo concluído com sucesso!")

# Executar o script
if __name__ == '__main__':
    main()
