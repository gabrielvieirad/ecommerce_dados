import pandas as pd
import numpy as np
import re
from pathlib import Path

BASE_DIR = Path(r"C:\Users\new big\Desktop\projeto_ecommerce_dados")
DADOS_DIR = BASE_DIR / "dados" / "csv_marketplaces" / "padronizados"
PROC_DIR  = BASE_DIR / "processados"
OUT_DIR   = PROC_DIR / "padronizados"
OUT_DIR.mkdir(parents=True, exist_ok=True)


# Entradas esperadas (ajuste se necessário)
AMZ_FILE   = DADOS_DIR / "amz.csv"                 # Amazon (SEM data)
BLZ_FILE   = DADOS_DIR / "blz_padronizado.xlsx"                # Beleza na Web
ML_FILE    = DADOS_DIR  / "mercadolivre_padronizado.csv"         # Mercado Livre (já extraído/limpo previamente)
SHP_FILE   = DADOS_DIR  / "shopee_padronizado.csv"       # Shopee (já consolidado por mês)
TTK_FILE   = DADOS_DIR  / "tiktok_market.csv"       # TikTok (já consolidado por mês)

# Entradas esperadas (ajuste se necessário)
AMZ_FILE   = DADOS_DIR / "amz.csv"                 # Amazon (SEM data)
BLZ_FILE   = DADOS_DIR / "blz_padronizado.xlsx"                # Beleza na Web
ML_FILE    = DADOS_DIR  / "mercadolivre_padronizado.csv"         # Mercado Livre (já extraído/limpo previamente)
SHP_FILE   = DADOS_DIR  / "shopee_padronizado.csv"       # Shopee (já consolidado por mês)
TTK_FILE   = DADOS_DIR  / "tiktok_market.csv"       # TikTok (já consolidado por mês)

# Saídas individuais
AMZ_OUT = OUT_DIR / "amazon_padronizado.csv"
BLZ_OUT = OUT_DIR / "blz_padronizado.csv"
ML_OUT  = OUT_DIR / "mercadolivre_padronizado_certo.csv"
SHP_OUT = OUT_DIR / "shopee_padronizado_certo.csv"
TTK_OUT = OUT_DIR / "tiktok_padronizado.csv"

# Saída unificada
MERGE_OUT = PROC_DIR / "dados_gerais.csv"

def padronizar_generico(nome, caminho_arquivo, canal):
    """Função genérica para padronizar marketplaces com checagem direta das colunas padrão."""
    if not caminho_arquivo.exists():
        print(f"[{canal.upper()}] Arquivo não encontrado: {caminho_arquivo}")
        return pd.DataFrame(columns=["sku","produto","ano","mes","canal","vendas","valor_total"])

    print(f"[{canal.upper()}] Lendo {caminho_arquivo}")
    df = pd.read_excel(caminho_arquivo) if caminho_arquivo.suffix in [".xlsx", ".xls"] else pd.read_csv(caminho_arquivo, encoding="utf-8", sep=None, engine="python")
    
    # Normaliza cabeçalhos e caracteres estranhos
    df.columns = df.columns.astype(str).str.strip().str.lower().str.replace(";", "").str.replace(" ", "_")

    # Se já tem as colunas padrão
    if set(["sku","produto","vendas","valor_total"]).issubset(df.columns):
        print(f"[{canal.upper()}] Colunas padrão detectadas, prosseguindo normalmente.")
        df = df[["sku","produto","vendas","valor_total"]].copy()

    else:
        # fallback: tentativa de mapear colunas equivalentes
        col_sku   = next((c for c in df.columns if "sku" in c), None)
        col_prod  = next((c for c in df.columns if "prod" in c or "descr" in c), None)
        col_qtd   = next((c for c in df.columns if "quant" in c or "unid" in c or "venda" in c), None)
        col_total = next((c for c in df.columns if "total" in c or "valor" in c or "amount" in c), None)
        miss = [n for n,v in {"sku":col_sku,"produto":col_prod,"vendas":col_qtd,"valor_total":col_total}.items() if v is None]
        if miss:
            print(f"[{canal.upper()}] Colunas não encontradas: {miss}. Tentando fallback minimalista.")
        df = df.rename(columns={
            col_sku: "sku",
            col_prod: "produto",
            col_qtd: "vendas",
            col_total: "valor_total"
        })

    df = remove_revisar_rows(df)
    df["canal"] = canal.lower()

    # se não houver ano/mes, gerar padrão
    if "ano" not in df.columns or "mes" not in df.columns:
        df["ano"] = 2025
        df["mes"] = 1

    df = pos_agrupamento(df)
    out_path = OUT_DIR / f"{canal.lower()}_padronizado.csv"
    df.to_csv(out_path, index=False, encoding="utf-8")
    print(f"[{canal.upper()}] OK: {len(df)} linhas salvas em {out_path}")
    return df

def clean_text(s):
    if s is None or (isinstance(s, float) and np.isnan(s)):
        return ""
    return str(s).strip()

def to_float(x):
    if pd.isna(x): 
        return 0.0
    s = str(x).strip()
    if s == "" or s == "-" or s.lower() == "nan":
        return 0.0
    s = re.sub(r"[^\d,.\-]", "", s)  # remove símbolos
    s = s.replace(",", ".")
    try:
        return float(s)
    except:
        return 0.0

def to_int(x):
    # remove .0 e converte para inteiro
    if pd.isna(x): 
        return 0
    s = str(x).strip()
    if s in ("", "-", "nan", "None"):
        return 0
    s = re.sub(r"[^\d\.-]", "", s)
    # se vier 12.0 -> 12
    try:
        v = float(s.replace(",", "."))
        return int(round(v))
    except:
        # fallback: remove tudo que não é dígito e tenta de novo
        s2 = re.sub(r"[^\d]", "", s)
        if s2 == "":
            return 0
        return int(s2)

def remove_revisar_rows(df):
    # remove qualquer linha que contenha "REVISAR" em colunas de texto
    mask = pd.Series(False, index=df.index)
    for col in df.columns:
        if df[col].dtype == "object":
            mask = mask | df[col].str.contains("REVISAR", case=False, na=False)
    return df[~mask].copy()

def pos_agrupamento(df):
    # pós-limpeza comum: filtra SKU vazio, força tipos e arredonda
    df["sku"] = df["sku"].astype(str).str.strip().str.upper()
    df = df[df["sku"] != ""].copy()
    # vendas inteiro, valor_total duas casas
    df["vendas"] = df["vendas"].apply(to_int)
    df["valor_total"] = df["valor_total"].apply(to_float).round(2)
    # remove linhas totalmente sem valor
    df = df[~((df["vendas"] <= 0) & (df["valor_total"] <= 0))].copy()
    # agrupar por sku+ano+mes+canal somando vendas e valor_total
    # produto: pega o primeiro não vazio dentro do grupo
    def first_non_empty(series):
        for v in series:
            v = clean_text(v)
            if v:
                return v
        return ""
    grouped = (df.groupby(["sku", "ano", "mes", "canal"], as_index=False)
                 .agg({"vendas": "sum", "valor_total": "sum", "produto": first_non_empty}))
    # ordena
    grouped = grouped[["sku","produto","ano","mes","canal","vendas","valor_total"]]
    # garante tipos finais
    grouped["vendas"] = grouped["vendas"].astype(int)
    grouped["valor_total"] = grouped["valor_total"].round(2)
    return grouped

def distribuir_amazon_ano_mes(df_amz):
    """
    Sem data na Amazon: distribuir proporcionalmente por períodos:
    - 1/3: 2023 (out, nov, dez)
    - 1/3: 2024 (jan..dez)
    - 1/3: 2025 (jan..ago)
    Distribuição por linha, ciclando pelos meses-alvo.
    """
    if df_amz.empty:
        df_amz["ano"] = []
        df_amz["mes"] = []
        return df_amz

    bucket_2023 = [(2023, 10), (2023, 11), (2023, 12)]
    bucket_2024 = [(2024, m) for m in range(1, 13)]
    bucket_2025 = [(2025, m) for m in range(1, 9)]

    total = len(df_amz)
    n1 = total // 3
    n2 = total // 3
    n3 = total - (n1 + n2)

    seq = []
    # distribui ciclando meses de cada bucket
    def fill_seq(n, months):
        out = []
        i = 0
        for _ in range(n):
            out.append(months[i % len(months)])
            i += 1
        return out

    seq.extend(fill_seq(n1, bucket_2023))
    seq.extend(fill_seq(n2, bucket_2024))
    seq.extend(fill_seq(n3, bucket_2025))

    # se por acaso sobrar ou faltar, ajusta
    if len(seq) < total:
        seq.extend(fill_seq(total - len(seq), bucket_2024))
    elif len(seq) > total:
        seq = seq[:total]

    anos, meses = zip(*seq) if seq else ([],[])
    df_amz["ano"] = list(anos)
    df_amz["mes"] = list(meses)
    return df_amz

"""def padronizar_amazon():
    if not AMZ_FILE.exists():
        print(f"[AMAZON] Arquivo não encontrado: {AMZ_FILE}")
        return pd.DataFrame(columns=["sku","produto","ano","mes","canal","vendas","valor_total"])
    print(f"[AMAZON] Lendo {AMZ_FILE}")
    df = pd.read_csv(AMZ_FILE, encoding="utf-8", sep=",")
    df.columns = [c.strip().lower() for c in df.columns]

    # Verificação direta — se já tem as colunas corretas, pula o fallback
    if set(["sku","produto","vendas","valor_total"]).issubset(df.columns):
        print("[AMAZON] Colunas padrão detectadas, prosseguindo normalmente.")
        df = df[["sku","produto","vendas","valor_total"]].copy()
    else:
        # Caso o nome das colunas varie
        col_sku   = next((c for c in df.columns if "sku" in c), None)
        col_prod  = next((c for c in df.columns if "prod" in c or "product" in c), None)
        col_qtd   = next((c for c in df.columns if "quant" in c or "units" in c or "venda" in c), None)
        col_total = next((c for c in df.columns if "total" in c or "amount" in c or "valor" in c), None)

        miss = [n for n,v in {"sku":col_sku,"produto":col_prod,"vendas":col_qtd,"valor_total":col_total}.items() if v is None]
        if miss:
            print(f"[AMAZON] Colunas não encontradas: {miss}. Tentando fallback minimalista.")
            df = df.rename(columns={
                col_sku: "sku",
                col_prod: "produto",
                col_qtd: "vendas",
                col_total: "valor_total"
            })
        else:
            df = df[[col_sku,col_prod,col_qtd,col_total]].copy()
            df.columns = ["sku","produto","vendas","valor_total"]

    # limpeza básica
    df = remove_revisar_rows(df)
    df["canal"] = "amazon"

    # sem data -> distribuir
    df = distribuir_amazon_ano_mes(df)

    # pós-agrupamento padrão
    df = pos_agrupamento(df)
    df.to_csv(AMZ_OUT, index=False, encoding="utf-8")
    print(f"[AMAZON] OK: {len(df)} linhas salvas em {AMZ_OUT}")
    return df

def padronizar_blz():
    if not BLZ_FILE.exists():
        print(f"[BLZ] Arquivo não encontrado: {BLZ_FILE}")
        return pd.DataFrame(columns=["sku","produto","ano","mes","canal","vendas","valor_total"])
    print(f"[BLZ] Lendo {BLZ_FILE}")
    df = pd.read_excel(BLZ_FILE, engine="openpyxl")
    df.columns = [c.strip().lower() for c in df.columns]

    # pick columns
    col_data  = next((c for c in df.columns if "data" in c), None)
    col_sku   = next((c for c in df.columns if "sku" in c), None)
    col_prod  = next((c for c in df.columns if "produto" in c or "descri" in c), None)
    col_qtd   = next((c for c in df.columns if "quant" in c), None)
    col_total = next((c for c in df.columns if "valor total" in c or (c.startswith("total"))), None)

    miss = [n for n,v in {"data":col_data, "sku":col_sku, "produto":col_prod, "vendas":col_qtd, "valor_total":col_total}.items() if v is None]
    if miss:
        print(f"[BLZ] Colunas não encontradas: {miss}.")
        return pd.DataFrame(columns=["sku","produto","ano","mes","canal","vendas","valor_total"])

    df = df[[col_data,col_sku,col_prod,col_qtd,col_total]].copy()
    df.columns = ["data","sku","produto","vendas","valor_total"]
    df = remove_revisar_rows(df)

    # quebra data em ano/mes
    df["data"] = pd.to_datetime(df["data"], errors="coerce")
    df = df[~df["data"].isna()].copy()
    df["ano"] = df["data"].dt.year.astype(int)
    df["mes"] = df["data"].dt.month.astype(int)
    df["canal"] = "beleza_na_web"
    df.drop(columns=["data"], inplace=True)

    df = pos_agrupamento(df)
    df.to_csv(BLZ_OUT, index=False, encoding="utf-8")
    print(f"[BLZ] OK: {len(df)} linhas salvas em {BLZ_OUT}")
    return df

def padronizar_meli():
    if not ML_FILE.exists():
        print(f"[ML] Arquivo não encontrado: {ML_FILE}")
        return pd.DataFrame(columns=["sku","produto","ano","mes","canal","vendas","valor_total"])
    print(f"[ML] Lendo {ML_FILE}")
    df = pd.read_csv(ML_FILE, encoding="utf-8")
    # Esperado já ter colunas: sku, produto (pode estar vazio), ano, mes, canal, vendas, valor_total
    # Limpeza pedida: vendas -> inteiro; valor_total -> 2 casas.
    df = remove_revisar_rows(df)
    # garante colunas
    for c in ["sku","produto","ano","mes","canal","vendas","valor_total"]:
        if c not in df.columns:
            df[c] = np.nan
    df = df[["sku","produto","ano","mes","canal","vendas","valor_total"]].copy()
    # tipos
    df["ano"] = pd.to_numeric(df["ano"], errors="coerce").fillna(0).astype(int)
    df["mes"] = pd.to_numeric(df["mes"], errors="coerce").fillna(0).astype(int)
    df["canal"] = df["canal"].fillna("mercadolivre")
    # conversões pedidas
    df["vendas"] = df["vendas"].apply(to_int)
    df["valor_total"] = df["valor_total"].apply(to_float).round(2)
    # pós-agrupamento
    df = pos_agrupamento(df)
    df.to_csv(ML_OUT, index=False, encoding="utf-8")
    print(f"[ML] OK: {len(df)} linhas salvas em {ML_OUT}")
    return df

def padronizar_shopee():
    if not SHP_FILE.exists():
        print(f"[SHOPEE] Arquivo não encontrado: {SHP_FILE}")
        return pd.DataFrame(columns=["sku","produto","ano","mes","canal","vendas","valor_total"])
    print(f"[SHOPEE] Lendo {SHP_FILE}")
    df = pd.read_csv(SHP_FILE, encoding="utf-8")
    # Esperado já conter: sku, produto, ano, mes, canal, vendas, valor_total
    df = remove_revisar_rows(df)
    # garante colunas
    for c in ["sku","produto","ano","mes","canal","vendas","valor_total"]:
        if c not in df.columns:
            df[c] = np.nan
    df = df[["sku","produto","ano","mes","canal","vendas","valor_total"]].copy()
    df["canal"] = "shopee"
    # pós-agrupamento (agrupa SKU por mês/ano e soma SOMENTE os iguais)
    df = pos_agrupamento(df)
    df.to_csv(SHP_OUT, index=False, encoding="utf-8")
    print(f"[SHOPEE] OK: {len(df)} linhas salvas em {SHP_OUT}")
    return df

def padronizar_tiktok():
    if not TTK_FILE.exists():
        print(f"[TIKTOK] Arquivo não encontrado: {TTK_FILE}")
        return pd.DataFrame(columns=["sku","produto","ano","mes","canal","vendas","valor_total"])
    print(f"[TIKTOK] Lendo {TTK_FILE}")
    df = pd.read_csv(TTK_FILE, encoding="utf-8")
    # Esperado conter: sku, produto, ano, mes, canal, vendas, valor_total
    df = remove_revisar_rows(df)
    for c in ["sku","produto","ano","mes","canal","vendas","valor_total"]:
        if c not in df.columns:
            df[c] = np.nan
    df = df[["sku","produto","ano","mes","canal","vendas","valor_total"]].copy()
    df["canal"] = "tiktok"
    df = pos_agrupamento(df)
    df.to_csv(TTK_OUT, index=False, encoding="utf-8")
    print(f"[TIKTOK] OK: {len(df)} linhas salvas em {TTK_OUT}")
    return df
"""

def main():
    print("[INFO] Padronização global iniciada.")
    amz = padronizar_generico("Amazon", AMZ_FILE, "Amazon")
    blz = padronizar_generico("Beleza_na_web", BLZ_FILE, "Beleza_na_web")
    ml  = padronizar_generico("Mercado_Livre", ML_FILE, "Mercado_Livre")
    shp = padronizar_generico("Shopee", SHP_FILE, "Shopee")
    ttk = padronizar_generico("TikTok", TTK_FILE, "TikTok")

    bases = [amz, blz, ml, shp, ttk]
    df_final = pd.concat(bases, ignore_index=True)
    df_final.to_csv(PROC_DIR / "dados_gerais.csv", index=False, encoding="utf-8")

    resumo = df_final.groupby("canal").agg(
        linhas=("sku","count"),
        skus_unicos=("sku","nunique"),
        meses=("mes","nunique"),
        anos=("ano","nunique")
    ).reset_index()

    print("[OK] Base unificada salva em:", PROC_DIR / "dados_gerais.csv")
    print("[RESUMO POR CANAL]")
    print(resumo)

if __name__ == "__main__":
    main()
