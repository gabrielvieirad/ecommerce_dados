import pandas as pd
import os
import re

arquivo_banco = r"C:\Users\new big\Desktop\projeto_ecommerce_dados\scripts_complementar\banco_de_dados_produtos.csv"
arquivo_destino = r"C:\Users\new big\Desktop\projeto_ecommerce_dados\dados\csv_marketplaces\mercadolivre_agrupado.csv"


def limpar_colunas(df):
    novas = []
    for c in df.columns:
        c_limpo = re.sub(r'\s+', '', str(c)).lower().strip().replace('"', '').replace("'", "")
        novas.append(c_limpo)
    df.columns = novas
    return df

def ler_arquivo_flexivel(caminho):
    if not os.path.exists(caminho):
        raise FileNotFoundError(f"Arquivo não encontrado: {caminho}")
    ext = os.path.splitext(caminho)[1].lower()

    if ext == ".csv":
        try:
            df = pd.read_csv(caminho, sep=";", dtype=str)
            if len(df.columns) == 1:  # caso tenha lido tudo numa coluna só
                df = pd.read_csv(caminho, sep=",", dtype=str)
        except:
            df = pd.read_csv(caminho, sep=",", dtype=str)
        return df

    elif ext in [".xls", ".xlsx"]:
        try:
            df = pd.read_excel(caminho, dtype=str)
            # se tiver só uma coluna e ela parecer um CSV, relê como CSV
            if len(df.columns) == 1 and ',' in str(df.columns[0]):
                print("Arquivo XLSX contém estrutura de CSV — relendo como CSV com vírgula.")
                df = pd.read_csv(caminho, sep=",", dtype=str)
            elif len(df.columns) == 1 and ';' in str(df.columns[0]):
                print("Arquivo XLSX contém estrutura de CSV — relendo como CSV com ponto e vírgula.")
                df = pd.read_csv(caminho, sep=";", dtype=str)
            return df
        except Exception as e:
            raise ValueError(f"Erro ao ler {caminho}: {e}")

    else:
        raise ValueError(f"Formato não suportado: {ext}")

def salvar_arquivo(df, caminho):
    ext = os.path.splitext(caminho)[1].lower()
    if ext == ".csv":
        try:
            df.to_csv(caminho, sep=";", index=False)
        except:
            df.to_csv(caminho, sep=",", index=False)
    elif ext in [".xls", ".xlsx"]:
        df.to_excel(caminho, index=False)
    else:
        raise ValueError(f"Formato não suportado: {ext}")

print("Lendo arquivos...")
banco = ler_arquivo_flexivel(arquivo_banco)
destino = ler_arquivo_flexivel(arquivo_destino)

# Normaliza colunas
banco = limpar_colunas(banco)
destino = limpar_colunas(destino)

print("Colunas normalizadas do destino:", destino.columns.tolist())

if "sku" not in banco.columns or "descricao" not in banco.columns:
    raise KeyError(f"Colunas esperadas no banco: 'Descricao' e 'sku'. Encontradas: {banco.columns.tolist()}")
if "sku" not in destino.columns or "produto" not in destino.columns:
    raise KeyError(f"Colunas esperadas no destino: 'produto' e 'sku'. Encontradas: {destino.columns.tolist()}")

print("Mesclando informações...")
mapa_sku = dict(zip(banco["sku"], banco["descricao"]))
destino["produto"] = destino["sku"].apply(lambda s: mapa_sku.get(s, "REVISAR"))

print("Salvando alterações...")
salvar_arquivo(destino, arquivo_destino)
print("Processo concluído com sucesso.")
