import pandas as pd
from pathlib import Path
import re

BASE_DIR = Path(__file__).resolve().parents[1]
AMZ_FILE = BASE_DIR / "dados" / "csv_marketplaces" / "amz.csv"
OUT_DIR = BASE_DIR / "processados"
OUT_DIR.mkdir(exist_ok=True)

def to_num(x):
    if pd.isna(x): return 0.0
    x = str(x)
    x = re.sub(r"[^\d,.\-]", "", x).replace(",", ".")
    try: return float(x)
    except: return 0.0

print(f"[INFO] Lendo {AMZ_FILE}")
df = pd.read_csv(AMZ_FILE, encoding="utf-8", sep=",")

df.columns = [c.strip().lower() for c in df.columns]

col_data   = next((c for c in df.columns if "data" in c or "order date" in c), None)
col_sku    = next((c for c in df.columns if c.startswith("sku")), None)
col_prod   = next((c for c in df.columns if "produto" in c or "product" in c), None)
col_qtd    = next((c for c in df.columns if "quantidade" in c or "units" in c), None)
col_total  = next((c for c in df.columns if "total" in c or "amount" in c), None)

df = df[[col_data, col_sku, col_prod, col_qtd, col_total]].copy()
df.columns = ["data", "sku", "produto", "vendas", "valor_total"]

df["data"] = pd.to_datetime(df["data"], errors="coerce")
df["ano"] = df["data"].dt.year.fillna(0).astype(int)
df["mes"] = df["data"].dt.month.fillna(0).astype(int)

df["vendas"] = pd.to_numeric(df["vendas"], errors="coerce").fillna(0)
df["valor_total"] = df["valor_total"].apply(to_num)
df = df[df["valor_total"] > 0]

df["sku"] = df["sku"].astype(str).str.strip().str.upper()
df["produto"] = df["produto"].astype(str).str.strip()
df["canal"] = "amazon"

agg = (df.groupby(["sku","produto","ano","mes","canal"], as_index=False)
         .agg({"vendas":"sum","valor_total":"sum"}))
agg["valor_unitario_medio"] = agg["valor_total"] / agg["vendas"].replace(0,1)

OUT_FILE = OUT_DIR / "amazon_merged.csv"
agg.to_csv(OUT_FILE, index=False, encoding="utf-8")

print(f"[OK] Amazon consolidado: {len(agg)} linhas salvas em {OUT_FILE}")
print(agg.head(10))
