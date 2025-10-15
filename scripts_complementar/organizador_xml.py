import os
import shutil
import xml.etree.ElementTree as ET

# === CONFIGURAÇÕES ===
pasta_origem = r'C:\Users\new big\Desktop\projeto_ecommerce_dados\tiktok'
pasta_destino = r'C:\Users\new big\Desktop\projeto_ecommerce_dados\tiktok_certo'
arquivo_nfes = r'C:\Users\new big\Desktop\projeto_ecommerce_dados\scripts_complementar\nfes_alvo.txt'

dry_run = False  # coloque False quando quiser mover de verdade

# === FUNÇÃO PARA NORMALIZAR NÚMEROS ===
def normalizar_numero(n):
    if not n:
        return ""
    return n.strip().lstrip("0") or "0"

# === LER TXT ===
with open(arquivo_nfes, 'r', encoding='utf-8') as f:
    nfes_alvo = [normalizar_numero(linha) for linha in f if linha.strip()]

nfes_alvo_set = set(nfes_alvo)
print(f"🔍 {len(nfes_alvo_set)} NFes carregadas do TXT (normalizadas)")
print(f"Exemplo: {list(nfes_alvo_set)[:10]}")

# === GARANTIR PASTA DESTINO ===
os.makedirs(pasta_destino, exist_ok=True)

# === FUNÇÃO PARA PEGAR nNF COM NAMESPACE ===
def extrair_numero_nfe(caminho_xml):
    try:
        tree = ET.parse(caminho_xml)
        root = tree.getroot()
        ns = {'nfe': root.tag.split('}')[0].strip('{')}
        tag = root.find('.//nfe:nNF', ns)
        if tag is not None and tag.text:
            return normalizar_numero(tag.text)
    except Exception:
        return None
    return None

# === LOOP PRINCIPAL ===
movidos = 0
checados = 0

for arquivo in os.listdir(pasta_origem):
    if not arquivo.lower().endswith('.xml'):
        continue

    caminho_xml = os.path.join(pasta_origem, arquivo)
    numero_nfe = extrair_numero_nfe(caminho_xml)
    checados += 1

    if not numero_nfe:
        print(f"⚠ {arquivo}: não encontrou <nNF>")
        continue

    if numero_nfe in nfes_alvo_set:
        print(f"✔ Match exato: {arquivo} — NFe {numero_nfe} {'(simulado)' if dry_run else '(movida)'}")
        if not dry_run:
            shutil.move(caminho_xml, os.path.join(pasta_destino, arquivo))
        movidos += 1
    else:
        print(f"→ Sem match: {arquivo} — NFe {numero_nfe}")

print("\n===== RESUMO =====")
print(f"Arquivos verificados: {checados}")
print(f"Arquivos movidos: {movidos}")
print(f"Dry run: {dry_run}")
print("==================")
