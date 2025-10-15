import pandas as pd
from pathlib import Path
import re
import numpy as np

BASE_DIR = Path(__file__).resolve().parents[1]
MARKET_DIR = BASE_DIR / "dados" / "csv_marketplaces"
OUT_DIR = BASE_DIR / "processados"
OUT_DIR.mkdir(exist_ok=True)

#Funções utilitárias


def log(msg, tipo="info"):
    cores = {"ok": "\033[92m", "warn": "\033[93m", "erro": "\033[91m", "info": "\033[94m", "end": "\033[0m"}
    prefix = {
        "ok": "[OK]   ",
        "warn": "[AVISO]",
        "erro": "[ERRO] ",
        "info": "[INFO] "
    }.get(tipo, "[INFO] ")
    print(f"{cores.get(tipo, '')}{prefix} {msg}{cores['end']}")

def normalizar_colunas(df):
    mapping = {
        'sku': ['sku', 'SKU', 'Código', 'Cod Produto', 'Product ID', 'product_id', 'cod_produto', 'codigo do produto'],
        'produto': ['produto', 'Product Name', 'nome', 'Descrição', 'product_title', 'nome do produto'],
        'vendas': ['vendas', 'Qtde Vendida', 'Quantidade', 'units_sold', 'Qtd', 'qtd vendida'],
        'valor_total': ['valor_total', 'faturamento', 'receita', 'Valor Total', 'valor_bruto', 'total_revenue', 'total'],
        'visualizacoes': ['visualizacoes', 'views', 'impressions', 'Visualizações', 'visualizações'],
        'devolucoes': ['devolucoes', 'returns', 'cancelamentos', 'Cancelamentos']
    }

    df.columns = df.columns.str.strip()
    for padrao, variantes in mapping.items():
        for var in variantes:
            if var in df.columns:
                df.rename(columns={var: padrao}, inplace=True)
                break
    return df

def limpar_valores(df):
    for col in ['vendas', 'valor_total', 'visualizacoes', 'devolucoes']:
        if col in df.columns:
            df[col] = (
                df[col]
                .astype(str)
                .str.replace(r'[^\d,.-]', '', regex=True)
                .str.replace(',', '.', regex=False)
            )
            df[col] = pd.to_numeric(df[col], errors='coerce')
    return df

def extrair_ano_mes(nome):
    nome = nome.lower()
    meses = {
        'jan': 1, 'fev': 2, 'mar': 3, 'abr': 4, 'mai': 5, 'jun': 6,
        'jul': 7, 'ago': 8, 'set': 9, 'out': 10, 'nov': 11, 'dez': 12
    }
    ano = re.search(r'20\d{2}', nome)
    mes = None
    for abv, num in meses.items():
        if abv in nome:
            mes = num
            break
    return (int(ano.group(0)) if ano else None, mes)

def identificar_canal(nome):
    nome = nome.lower()
    if any(x in nome for x in ["meli", "ml", "mercado"]):
        return "mercadolivre"
    if "amazon" in nome:
        return "amazon"
    if "tiktok" in nome:
        return "tiktok"
    if "blz" in nome or "beleza" in nome:
        return "beleza_na_web"
    if "shopee" in nome:
        return "shopee"
    return None

def carregar_arquivo(arquivo, canal):
    """Le CSV ou XLSX"""
    if arquivo.suffix in [".xlsx", ".xls"]:
        xls = pd.ExcelFile(arquivo)
        frames = []
        for aba in xls.sheet_names:
            aba_nome = aba.lower()
            if canal == "mercadolivre" and "negócio" not in aba_nome:
                continue
            try:
                df = pd.read_excel(xls, sheet_name=aba)
                frames.append(df)
            except Exception as e:
                log(f"Erro na aba {aba}: {e}", "warn")
        if frames:
            return pd.concat(frames, ignore_index=True)
    else:
        try:
            df = pd.read_csv(arquivo, sep=None, engine="python", encoding="utf-8", low_memory=False)
            return df
        except Exception:
            # tenta com ponto e vírgula
            df = pd.read_csv(arquivo, sep=";", encoding="utf-8", low_memory=False)
            return df
    return pd.DataFrame()


# Leitura e padronização dos dados

frames_total = []

# Pastas
pasta_dados = MARKET_DIR / "dados"
pasta_shopee = MARKET_DIR / "shopee"

for arquivo in list(pasta_dados.glob("*.*")) + list(pasta_shopee.glob("*.xlsx")):
    canal = identificar_canal(arquivo.name)
    if not canal:
        log(f"Ignorando {arquivo.name} (canal não reconhecido)", "warn")
        continue

    # ignora arquivos sem ano (ex: guias, campanhas)
    if not re.search(r'20\d{2}', arquivo.name):
        log(f"Ignorando {arquivo.name} (sem ano no nome)", "warn")
        continue

    log(f"Lendo {arquivo.name} ({canal.upper()})", "info")

    try:
        df = carregar_arquivo(arquivo, canal)
        if df.empty:
            log(f"{arquivo.name} sem dados válidos", "warn")
            continue

        df = normalizar_colunas(df)
        df = limpar_valores(df)
        ano, mes = extrair_ano_mes(arquivo.name)
        df["ano"] = ano
        df["mes"] = mes
        df["canal"] = canal

        # Garante colunas mínimas
        for col in ["sku", "produto", "vendas", "valor_total", "visualizacoes", "devolucoes"]:
            if col not in df.columns:
                df[col] = np.nan

        frames_total.append(df)
        log(f"{arquivo.name}: {len(df)} linhas importadas", "ok")

    except Exception as e:
        log(f"Falha ao processar {arquivo.name}: {e}", "erro")

# Consolidação final

if not frames_total:
    log("Nenhum arquivo foi processado com sucesso.", "erro")
else:
    df_final = pd.concat(frames_total, ignore_index=True)
    colunas_ordenadas = ["sku", "produto", "vendas", "valor_total", "visualizacoes", "devolucoes", "ano", "mes", "canal"]
    df_final = df_final[colunas_ordenadas]
    df_final.to_csv(OUT_DIR / "marketplaces.csv", index=False, encoding="utf-8")

    log(f"Arquivo consolidado salvo em: {OUT_DIR / 'marketplaces.csv'}", "ok")
    log(f"Total de linhas: {len(df_final)}", "info")

    resumo = df_final.groupby("canal").size().reset_index(name="linhas")
    print("\nResumo por canal:\n", resumo)
