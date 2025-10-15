import pandas as pd
import sqlite3
from pathlib import Path

# Caminhos
BASE_DIR = Path(__file__).resolve().parents[1]
DB_PATH = BASE_DIR / "database" / "tiny_data.db"
DATA_DIR = BASE_DIR / "processados"

# Arquivos CSV
csv_files = {
    "vendas": DATA_DIR / "vendas.csv",
    "produtos": DATA_DIR / "produtos.csv",
    "clientes": DATA_DIR / "clientes.csv",
    "tiny_merged": DATA_DIR / "tiny_merged.csv"
}

# Conectar ao SQLite
conn = sqlite3.connect(DB_PATH)
print(f"[DB] Conectado a {DB_PATH}")

# Importar cada CSV como tabela
for nome, caminho in csv_files.items():
    if caminho.exists():
        df = pd.read_csv(caminho)
        df.to_sql(nome, conn, if_exists="replace", index=False)
        print(f"[DB] Tabela '{nome}' importada ({len(df)} registros).")
    else:
        print(f"[AVISO] Arquivo não encontrado: {caminho}")

conn.close()
print("[DB] Banco atualizado com sucesso!")
