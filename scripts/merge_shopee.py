import pandas as pd
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
SHOPEE_DIR = BASE_DIR / "dados" / "csv_marketplaces" / "shopee"
OUT_DIR = BASE_DIR / "processados"
OUT_DIR.mkdir(exist_ok=True)

print(f"[INFO] Lendo arquivos da Shopee em {SHOPEE_DIR}")

# === Lista todos os .xlsx nas subpastas (2024, 2025 etc.) ===
arquivos = list(SHOPEE_DIR.rglob("*.xlsx"))
if not arquivos:
    print("[AVISO] Nenhum arquivo .xlsx encontrado nas pastas da Shopee.")
    exit()

print(f"[INFO] {len(arquivos)} arquivos encontrados.")

dados = []

for file in arquivos:
    try:
        df = pd.read_excel(file, engine="openpyxl")

        # Verifica se contém as colunas esperadas
        colunas = df.columns.str.strip()

        # Identifica colunas relevantes (com tolerância a variações de nome)
        col_mapeadas = {
            "sku": None,
            "produto": None,
            "vendas": None,
            "valor_total": None,
            "data": None
        }

        for c in colunas:
            c_lower = c.lower()
            if "sku" in c_lower and "principal" in c_lower:
                col_mapeadas["sku"] = c
            elif c_lower.startswith("sku") and not col_mapeadas["sku"]:
                col_mapeadas["sku"] = c
            elif "produto" in c_lower:
                col_mapeadas["produto"] = c
            elif "quantidade" in c_lower:
                col_mapeadas["vendas"] = c
            elif "valor total" in c_lower:
                col_mapeadas["valor_total"] = c
            elif "data de criação" in c_lower:
                col_mapeadas["data"] = c

        # Se alguma coluna essencial estiver ausente, pula o arquivo
        if not all(col_mapeadas.values()):
            print(f"[AVISO] Colunas principais não encontradas em {file.name}. Pulando.")
            continue

        df_limpo = df[list(col_mapeadas.values())].copy()
        df_limpo.columns = ["sku", "produto", "vendas", "valor_total", "data"]

        # Extrai ano e mês da coluna de data
        df_limpo["data"] = pd.to_datetime(df_limpo["data"], errors="coerce")
        df_limpo["ano"] = df_limpo["data"].dt.year
        df_limpo["mes"] = df_limpo["data"].dt.month

        # Limpeza e conversões
        df_limpo["sku"] = df_limpo["sku"].astype(str).str.strip().str.upper()
        df_limpo["produto"] = df_limpo["produto"].astype(str).str.strip()
        df_limpo["vendas"] = pd.to_numeric(df_limpo["vendas"], errors="coerce").fillna(0)
        df_limpo["valor_total"] = (
            df_limpo["valor_total"]
            .astype(str)
            .str.replace("[^0-9,.-]", "", regex=True)
            .str.replace(",", ".")
            .astype(float)
            .fillna(0)
        )

        df_limpo["canal"] = "shopee"

        dados.append(df_limpo)

        print(f"[OK] {file.name}: {len(df_limpo)} linhas importadas.")

    except Exception as e:
        print(f"[ERRO] Falha ao ler {file.name}: {e}")

# === Consolidação ===
if not dados:
    print("[AVISO] Nenhum dado consolidado.")
    exit()

df_final = pd.concat(dados, ignore_index=True)

# Agrupar por SKU / Produto / Ano / Mês
df_grouped = (
    df_final.groupby(["sku", "produto", "ano", "mes", "canal"], as_index=False)
    .agg({"vendas": "sum", "valor_total": "sum"})
)
df_grouped["valor_unitario_medio"] = df_grouped["valor_total"] / df_grouped["vendas"]

# Salva resultado
OUT_FILE = OUT_DIR / "shopee_merged.csv"
df_grouped.to_csv(OUT_FILE, index=False, encoding="utf-8")
print(f"[OK] Shopee consolidado: {len(df_grouped)} linhas salvas em {OUT_FILE}")
print(df_grouped.head(10))
