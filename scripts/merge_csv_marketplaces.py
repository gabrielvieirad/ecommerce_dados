import pandas as pd
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
PROC_DIR = BASE_DIR / "processados"
DB_DIR = BASE_DIR / "database"

# === Caminhos das bases ===
tiny_file = PROC_DIR / "tiny_merged.csv"
market_file = PROC_DIR / "marketplaces.csv"
tiktok_file = PROC_DIR / "tiktok_market.csv"
OUT_FILE = PROC_DIR / "dados_gerais.csv"

print(f"[INFO] Iniciando merge geral...")
print(f"[INFO] Diretório base: {PROC_DIR}")

# === Carrega cada base ===
try:
    tiny = pd.read_csv(tiny_file, encoding="utf-8", low_memory=False)
    print(f"[OK] Tiny ERP: {len(tiny)} registros")
except Exception as e:
    print(f"[ERRO] Falha ao carregar {tiny_file}: {e}")
    tiny = pd.DataFrame()

try:
    market = pd.read_csv(market_file, encoding="utf-8", low_memory=False)
    print(f"[OK] Marketplaces: {len(market)} registros")
except Exception as e:
    print(f"[ERRO] Falha ao carregar {market_file}: {e}")
    market = pd.DataFrame()

try:
    tiktok = pd.read_csv(tiktok_file, encoding="utf-8", low_memory=False)
    print(f"[OK] TikTok Shop: {len(tiktok)} registros")
except Exception as e:
    print(f"[ERRO] Falha ao carregar {tiktok_file}: {e}")
    tiktok = pd.DataFrame()

# === Normaliza colunas ===
for df in [tiny, market, tiktok]:
    if "sku" in df.columns:
        df["sku"] = df["sku"].astype(str).str.strip().str.upper()

# === Padroniza colunas principais ===
cols_base = ["sku", "produto", "vendas", "valor_total", "ano", "mes", "canal"]

for df in [tiny, market, tiktok]:
    for col in cols_base:
        if col not in df.columns:
            df[col] = None
    df = df[cols_base]

# === Concatena tudo ===
merged = pd.concat([tiny, market, tiktok], ignore_index=True)
merged = merged.dropna(subset=["sku"])

print(f"[INFO] Total combinado: {len(merged)} registros antes da consolidação")

# === Consolida duplicações (SKU / canal / ano / mes) ===
consolidado = (
    merged.groupby(["sku", "produto", "canal", "ano", "mes"], as_index=False)
          .agg({"vendas": "sum", "valor_total": "sum"})
)
consolidado["valor_unitario_medio"] = consolidado["valor_total"] / consolidado["vendas"]

# === Exporta resultado final ===
consolidado.to_csv(OUT_FILE, index=False, encoding="utf-8")
print(f"[OK] Base final integrada salva em: {OUT_FILE}")
print(f"[OK] Total final: {len(consolidado)} linhas consolidadas")
print(consolidado.head(10))
