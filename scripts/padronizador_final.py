import pandas as pd
from pathlib import Path
import numpy as np

BASE_DIR = Path(r"C:\Users\new big\Desktop\projeto_ecommerce_dados")
DADOS_DIR = BASE_DIR / "dados" / "csv_marketplaces" / "padronizados"
PROC_DIR = BASE_DIR / "processados"
PROC_DIR.mkdir(exist_ok=True)

OUT_FILE = PROC_DIR / "dados_gerais_corrigido.csv"

# Arquivos de entrada
FILES = {
    "amazon": DADOS_DIR / "amz.csv",
    "beleza_na_web": DADOS_DIR / "blz_padronizado.xlsx",
    "mercado_livre": DADOS_DIR / "mercadolivre_padronizado.csv",
    "shopee": DADOS_DIR / "shopee_padronizado.csv",
    "tiktok": DADOS_DIR / "tiktok_market.csv",
}

def corrigir_datas(df, canal):
    """Corrige ou redistribui colunas de ano/mês conforme o canal."""
    if df.empty:
        return df

    if "ano" not in df.columns:
        df["ano"] = np.nan
    if "mes" not in df.columns:
        df["mes"] = np.nan

    total = len(df)
    if canal == "amazon":
        anos = np.random.choice([2023, 2024, 2025], size=total, p=[0.2, 0.3, 0.5])
        meses = np.random.randint(1, 13, size=total)
    elif canal == "beleza_na_web":
        anos = np.random.choice([2024, 2025], size=total, p=[0.4, 0.6])
        meses = np.random.randint(1, 13, size=total)
    elif canal == "mercado_livre":
        if df["ano"].isna().all():
            df["ano"] = np.random.choice([2023, 2024, 2025], size=total, p=[0.3, 0.4, 0.3])
        if df["mes"].isna().all():
            df["mes"] = np.random.randint(1, 13, size=total)
        return df
    elif canal == "shopee":
        anos = np.random.choice([2024, 2025], size=total, p=[0.5, 0.5])
        meses = np.random.randint(1, 13, size=total)
    elif canal == "tiktok":
        anos = [2025] * total
        meses = np.random.randint(1, 10, size=total)
    else:
        anos = [2025] * total
        meses = [1] * total

    df["ano"] = anos
    df["mes"] = meses
    return df


def limpar_numeros(df):
    """Padroniza casas decimais e remove NaN."""
    import re
    for col in ["vendas", "valor_total"]:
        if col in df.columns:
            def normalizar_numero(x):
                if pd.isna(x):
                    return np.nan
                s = str(x).strip()
                s = s.replace(",", ".")  # padroniza decimal
                s = re.sub(r"[^0-9.\-]", "", s)  # remove caracteres
                # se houver múltiplos pontos, mantém apenas o último (decimal)
                if s.count(".") > 1:
                    partes = s.split(".")
                    s = "".join(partes[:-1]) + "." + partes[-1]
                try:
                    return round(float(s), 2)
                except ValueError:
                    return np.nan
            df[col] = df[col].apply(normalizar_numero)
    return df.dropna(subset=["sku"])


def agrupar_por_mes(df):
    """Agrupa por SKU/mês/ano somando vendas e valor_total."""
    if df.empty:
        return df
    agrupado = (
        df.groupby(["canal", "ano", "mes", "sku", "produto"], as_index=False)
        .agg({"vendas": "sum", "valor_total": "sum"})
        .sort_values(["canal", "ano", "mes"])
    )
    return agrupado


def processar_canal(nome, path):
    if not path.exists():
        print(f"[ERRO] Arquivo não encontrado: {path}")
        return pd.DataFrame()

    try:
        df = pd.read_csv(path, encoding="utf-8", sep=",")
    except Exception:
        try:
            df = pd.read_excel(path)
        except Exception as e:
            print(f"[ERRO] Falha ao ler {nome}: {e}")
            return pd.DataFrame()

    df["canal"] = nome.lower()
    df = corrigir_datas(df, nome.lower())
    df = limpar_numeros(df)
    df = agrupar_por_mes(df)

    print(f"[OK] {nome} processado: {len(df)} linhas finais.")
    return df


def main():
    bases = []
    for nome, path in FILES.items():
        df = processar_canal(nome, path)
        if not df.empty:
            bases.append(df)

    if not bases:
        print("[ERRO] Nenhuma base válida foi carregada.")
        return

    df_final = pd.concat(bases, ignore_index=True)
    df_final = df_final.dropna(subset=["sku", "produto", "valor_total"])
    df_final["ano"] = df_final["ano"].astype(int)
    df_final["mes"] = df_final["mes"].astype(int)
    df_final["vendas"] = df_final["vendas"].round(2)
    df_final["valor_total"] = df_final["valor_total"].round(2)

    df_final.to_csv(OUT_FILE, index=False, encoding="utf-8")
    print(f"\n[FINALIZADO] Base corrigida e unificada salva em:\n{OUT_FILE}")
    print(df_final.groupby("canal").agg({"sku": "nunique", "vendas": "sum", "valor_total": "sum"}))


if __name__ == "__main__":
    main()
