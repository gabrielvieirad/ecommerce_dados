import pandas as pd
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
PROC_DIR = BASE_DIR / "processados"
OUT_FILE = PROC_DIR / "dados_gerais.csv"

print("[INFO] Carregando bases...")

try:
    tiny = pd.read_csv(PROC_DIR / "tiny_merged.csv", encoding="utf-8", low_memory=False)
    market = pd.read_csv(PROC_DIR / "marketplaces.csv", encoding="utf-8", low_memory=False)
except Exception as e:
    print(f"[ERRO] Falha ao carregar arquivos: {e}")
    exit()

print(f"[OK] Tiny: {len(tiny)} linhas")
print(f"[OK] Marketplaces: {len(market)} linhas")

# Detectar nome da coluna SKU no Tiny
possiveis_colunas_sku = ["sku", "SKU", "codigo", "Código", "codigo_produto", "codigo produto", "id_produto"]

col_sku_tiny = None
for c in tiny.columns:
    if c.strip().lower() in [p.lower() for p in possiveis_colunas_sku]:
        col_sku_tiny = c
        break

if not col_sku_tiny:
    print("[ERRO] Nenhuma coluna equivalente a SKU encontrada no tiny_merged.csv.")
    print("Colunas disponíveis:", list(tiny.columns))
    exit()
else:
    print(f"[INFO] Coluna de SKU detectada no Tiny: '{col_sku_tiny}'")
    tiny.rename(columns={col_sku_tiny: "sku"}, inplace=True)

# Normalização de SKU
tiny["sku"] = tiny["sku"].astype(str).str.strip().str.upper()
market["sku"] = market["sku"].astype(str).str.strip().str.upper()

# Merge
print("[INFO] Realizando merge por SKU...")
merged = pd.merge(market, tiny, on="sku", how="left", suffixes=("_market", "_tiny"))

# Cria métricas consolidadas
if "valor_total_market" in merged.columns:
    merged["receita_total"] = merged["valor_total_market"].fillna(0)
if "valor_total_tiny" in merged.columns:
    merged["receita_tiny"] = merged["valor_total_tiny"].fillna(0)

# Fatura total por SKU
if "valor_total_market" in merged.columns:
    faturamento_canais = merged.groupby("sku")["valor_total_market"].sum().reset_index(name="faturamento_total_canais")
    merged = merged.merge(faturamento_canais, on="sku", how="left")

# Exporta resultado
merged.to_csv(OUT_FILE, index=False, encoding="utf-8")
print(f"Base consolidada salva em: {OUT_FILE}")
print(f"Total de linhas: {len(merged)}")

# Relatório rápido por canal
if "valor_total_market" in merged.columns and "canal" in merged.columns:
    resumo = merged.groupby("canal")["valor_total_market"].sum().reset_index()
    print("\nResumo de faturamento por canal:")
    print(resumo.to_string(index=False))
