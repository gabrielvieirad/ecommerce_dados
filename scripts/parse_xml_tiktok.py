import xml.etree.ElementTree as ET
import pandas as pd
from pathlib import Path
import re

BASE_DIR = Path(__file__).resolve().parents[1]
XML_DIR = BASE_DIR / "dados" / "tiktok_certo"
OUT_DIR = BASE_DIR / "processados"
OUT_DIR.mkdir(exist_ok=True)

def detect_default_namespace(root_tag: str):
    if root_tag.startswith("{") and "}" in root_tag:
        return root_tag[1:].split("}", 1)[0]
    return None

def strip_namespace_inplace(elem: ET.Element):
    for e in elem.iter():
        if '}' in e.tag:
            e.tag = e.tag.split('}', 1)[1]

def to_float(x):
    if x is None:
        return 0.0
    s = str(x)
    s = re.sub(r"[^\d,.\-]", "", s).replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return 0.0

def extract_with_namespace(tree: ET.ElementTree):
    root = tree.getroot()
    ns_uri = detect_default_namespace(root.tag)
    if not ns_uri:
        return []
    ns = {"ns": ns_uri}
    data_node = root.find(".//ns:ide/ns:dhEmi", ns)
    if (data_node is None) or (data_node.text is None):
        data_node = root.find(".//ns:ide/ns:dEmi", ns)
    data = data_node.text[:10] if (data_node is not None and data_node.text) else None

    registros = []
    for det in root.findall(".//ns:det", ns):
        prod = det.find("ns:prod", ns)
        if prod is None:
            continue
        sku = prod.findtext("ns:cProd", default=None, namespaces=ns)
        produto = prod.findtext("ns:xProd", default=None, namespaces=ns)
        qtd = prod.findtext("ns:qCom", default=None, namespaces=ns)
        vunit = prod.findtext("ns:vUnCom", default=None, namespaces=ns)
        vprod = prod.findtext("ns:vProd", default=None, namespaces=ns)

        registros.append({
            "sku": (str(sku).strip().upper() if sku else None),
            "produto": produto,
            "vendas": to_float(qtd),
            "valor_total": to_float(vprod),
            "valor_unitario": to_float(vunit),
            "ano": int(data[:4]) if data else None,
            "mes": int(data[5:7]) if data else None,
            "visualizacoes": 0,
            "devolucoes": 0,
            "canal": "tiktok"
        })
    return registros

def extract_without_namespace(tree: ET.ElementTree):
    root = tree.getroot()
    strip_namespace_inplace(root)
    data_node = root.find(".//dhEmi")
    if data_node is None:
        data_node = root.find(".//dEmi")
    data = data_node.text[:10] if (data_node is not None and data_node.text) else None

    registros = []
    for det in root.findall(".//det"):
        prod = det.find("prod")
        if prod is None:
            continue
        sku = prod.findtext("cProd")
        produto = prod.findtext("xProd")
        qtd = prod.findtext("qCom")
        vunit = prod.findtext("vUnCom")
        vprod = prod.findtext("vProd")

        registros.append({
            "sku": (str(sku).strip().upper() if sku else None),
            "produto": produto,
            "vendas": to_float(qtd),
            "valor_total": to_float(vprod),
            "valor_unitario": to_float(vunit),
            "ano": int(data[:4]) if data else None,
            "mes": int(data[5:7]) if data else None,
            "visualizacoes": 0,
            "devolucoes": 0,
            "canal": "tiktok"
        })
    return registros

def main():
    if not XML_DIR.exists():
        print(f"[ERRO] Pasta não encontrada: {XML_DIR}")
        return

    files = list(XML_DIR.rglob("*.xml"))
    print(f"[INFO] Lendo XMLs em: {XML_DIR} | arquivos encontrados: {len(files)}")
    if not files:
        print("[AVISO] Nenhum .xml encontrado nessa pasta.")
        return

    all_rows = []
    for xml_path in files:
        try:
            tree = ET.parse(xml_path)
        except Exception as e:
            print(f"[ERRO] Falha ao abrir {xml_path.name}: {e}")
            continue

        rows = extract_with_namespace(tree)
        if not rows:
            rows = extract_without_namespace(tree)

        if not rows:
            print(f"[AVISO] Sem itens detectados em: {xml_path.name}")
        else:
            all_rows.extend(rows)

    if not all_rows:
        print("[AVISO] Nenhum dado extraído dos XMLs do TikTok. Verifique estrutura/tags.")
        return

    df = pd.DataFrame(all_rows)
    df = df.dropna(subset=["sku"])
    df["sku"] = df["sku"].astype(str).str.strip().str.upper()

    # === NOVO BLOCO: CONSOLIDAÇÃO POR SKU / MÊS / ANO ===
    df_grouped = (
        df.groupby(["sku", "produto", "ano", "mes", "canal"], as_index=False)
          .agg({
              "vendas": "sum",
              "valor_total": "sum"
          })
    )
    df_grouped["valor_unitario_medio"] = df_grouped["valor_total"] / df_grouped["vendas"]

    # salva o CSV final já consolidado
    OUT_FILE = OUT_DIR / "tiktok_market.csv"
    df_grouped.to_csv(OUT_FILE, index=False, encoding="utf-8")

    print(f"[OK] TikTok consolidado: {len(df_grouped)} linhas (por SKU/mês) salvas em {OUT_FILE}")
    print(df_grouped.head(10))

if __name__ == "__main__":
    main()
