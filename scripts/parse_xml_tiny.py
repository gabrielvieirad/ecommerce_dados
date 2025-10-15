from __future__ import annotations

import sys
import traceback
from pathlib import Path
from typing import Dict, Any, List, Optional
import xml.etree.ElementTree as ET
from datetime import datetime
from dateutil import parser as dtparser
import pandas as pd


# ---------- Config ----------
BASE_DIR = Path(__file__).resolve().parents[1]  # raiz do projeto
RAW_DIRS = [
    BASE_DIR / "dados" / "xml_tiny" / "2024",
    BASE_DIR / "dados" / "xml_tiny" / "2025",
]
OUT_DIR = BASE_DIR / "processados"
OUT_DIR.mkdir(parents=True, exist_ok=True)

VENDAS_CSV = OUT_DIR / "vendas.csv"
PRODUTOS_CSV = OUT_DIR / "produtos.csv"
CLIENTES_CSV = OUT_DIR / "clientes.csv"
MERGED_CSV = OUT_DIR / "tiny_merged.csv"


# ---------- Helpers XML ----------
WILDCARD = "{*}"  # permite ignorar namespaces em buscas

def ftext(elem: ET.Element, path: str, default: Optional[str] = None) -> Optional[str]:
    """
    findtext com wildcard de namespace.
    path ex: './/{*}infNFe/{*}ide/{*}dhEmi'
    """
    if elem is None:
        return default
    val = elem.findtext(path)
    return val if val is not None else default

def find(elem: ET.Element, path: str) -> Optional[ET.Element]:
    return elem.find(path) if elem is not None else None

def first_nonempty(*vals: Optional[str]) -> Optional[str]:
    for v in vals:
        if v is not None and str(v).strip() != "":
            return v
    return None

def to_float(x: Optional[str]) -> Optional[float]:
    if x is None:
        return None
    s = str(x).strip().replace(",", ".")
    try:
        return float(s)
    except Exception:
        return None

def to_int(x: Optional[str]) -> Optional[int]:
    if x is None:
        return None
    try:
        return int(str(x).strip())
    except Exception:
        return None

def to_date_iso(x: Optional[str]) -> Optional[str]:
    if x is None or str(x).strip() == "":
        return None
    try:
        # dhEmi costuma vir como 2024-01-25T10:30:00-03:00
        dt = dtparser.parse(x)
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        # alguns XMLs trazem apenas data
        try:
            dt = datetime.strptime(x.strip()[:10], "%Y-%m-%d")
            return dt.strftime("%Y-%m-%d 00:00:00")
        except Exception:
            return None


# ---------- Parsers ----------
def parse_header(root: ET.Element) -> Dict[str, Any]:
    """
    Cabeçalho da nota (vendas)
    """
    # Suporte a NFe e NFCe: tanto infNFe -> ide quanto infNFeSupl em algumas variantes
    infNFe = find(root, f".//{WILDCARD}infNFe")
    ide = find(infNFe, f".//{WILDCARD}ide") if infNFe is not None else None
    emit = find(infNFe, f".//{WILDCARD}emit") if infNFe is not None else None
    dest = find(infNFe, f".//{WILDCARD}dest") if infNFe is not None else None
    total = find(infNFe, f".//{WILDCARD}total/{WILDCARD}ICMSTot") if infNFe is not None else None

    id_nota = None
    if infNFe is not None:
        id_nota = infNFe.get("Id")
        if id_nota and id_nota.startswith("NFe"):
            # normalizar sem "NFe"
            id_nota = id_nota.replace("NFe", "")

    numero_nota = ftext(ide, f".//{WILDCARD}nNF")
    serie = ftext(ide, f".//{WILDCARD}serie")
    modelo = ftext(ide, f".//{WILDCARD}mod")
    dhEmi = first_nonempty(
        ftext(ide, f".//{WILDCARD}dhEmi"),
        ftext(ide, f".//{WILDCARD}dEmi"),  # em alguns casos
    )
    data_emissao = to_date_iso(dhEmi)

    cnpj_emit = ftext(emit, f".//{WILDCARD}CNPJ")
    nome_emit = ftext(emit, f".//{WILDCARD}xNome")

    cpf_cli = ftext(dest, f".//{WILDCARD}CPF")
    cnpj_cli = ftext(dest, f".//{WILDCARD}CNPJ")

    vNF = to_float(ftext(total, f".//{WILDCARD}vNF"))
    vProd = to_float(ftext(total, f".//{WILDCARD}vProd"))
    vFrete = to_float(ftext(total, f".//{WILDCARD}vFrete"))
    vICMS = to_float(ftext(total, f".//{WILDCARD}vICMS"))
    vIPI = to_float(ftext(total, f".//{WILDCARD}vIPI"))
    vDesc = to_float(ftext(total, f".//{WILDCARD}vDesc"))

    # Forma de pagamento (opcional): tPag está dentro de pag/detPag ou pag
    tpag = first_nonempty(
        ftext(infNFe, f".//{WILDCARD}pag/{WILDCARD}detPag/{WILDCARD}tPag"),
        ftext(infNFe, f".//{WILDCARD}pag/{WILDCARD}tPag"),
    )

    return {
        "id_nota": id_nota,
        "modelo": modelo,
        "serie": serie,
        "numero_nota": numero_nota,
        "data_emissao": data_emissao,
        "cnpj_emitente": cnpj_emit,
        "nome_emitente": nome_emit,
        "cpf_cliente": cpf_cli,
        "cnpj_cliente": cnpj_cli,
        "valor_total": vNF,
        "valor_produtos": vProd,
        "valor_frete": vFrete,
        "valor_icms": vICMS,
        "valor_ipi": vIPI,
        "valor_desconto": vDesc,
        "forma_pgto": tpag,
    }


def parse_customer(root: ET.Element, id_nota: Optional[str]) -> Dict[str, Any]:
    infNFe = find(root, f".//{WILDCARD}infNFe")
    dest = find(infNFe, f".//{WILDCARD}dest") if infNFe is not None else None
    end_dest = find(dest, f".//{WILDCARD}enderDest") if dest is not None else None

    nome = ftext(dest, f".//{WILDCARD}xNome")
    cpf = ftext(dest, f".//{WILDCARD}CPF")
    cnpj = ftext(dest, f".//{WILDCARD}CNPJ")
    doc = first_nonempty(cpf, cnpj)

    return {
        "id_nota": id_nota,
        "nome_cliente": nome,
        "cpf_cnpj": doc,
        "endereco": ftext(end_dest, f".//{WILDCARD}xLgr"),
        "numero": ftext(end_dest, f".//{WILDCARD}nro"),
        "bairro": ftext(end_dest, f".//{WILDCARD}xBairro"),
        "cidade": ftext(end_dest, f".//{WILDCARD}xMun"),
        "uf": ftext(end_dest, f".//{WILDCARD}UF"),
        "cep": ftext(end_dest, f".//{WILDCARD}CEP"),
    }


def parse_items(root: ET.Element, id_nota: Optional[str]) -> List[Dict[str, Any]]:
    """
    Varre todos <det> (itens) da nota
    """
    infNFe = find(root, f".//{WILDCARD}infNFe")
    if infNFe is None:
        return []

    items = []
    for det in infNFe.findall(f".//{WILDCARD}det"):
        prod = find(det, f".//{WILDCARD}prod")
        imposto = find(det, f".//{WILDCARD}imposto")
        icms = find(imposto, f".//{WILDCARD}ICMS") if imposto is not None else None

        # ICMS pode vir dentro de vários nós (ICMS00, ICMS20, etc.). Buscamos por wildcard.
        cst = ftext(icms, f".//{WILDCARD}CST")
        pICMS = to_float(ftext(icms, f".//{WILDCARD}pICMS"))

        items.append({
            "id_nota": id_nota,
            "codigo_produto": ftext(prod, f".//{WILDCARD}cProd"),
            "nome_produto": ftext(prod, f".//{WILDCARD}xProd"),
            "ncm": ftext(prod, f".//{WILDCARD}NCM"),
            "cfop": ftext(prod, f".//{WILDCARD}CFOP"),
            "quantidade": to_float(ftext(prod, f".//{WILDCARD}qCom")),
            "valor_unitario": to_float(ftext(prod, f".//{WILDCARD}vUnCom")),
            "valor_total_item": to_float(ftext(prod, f".//{WILDCARD}vProd")),
            "cst_icms": cst,
            "aliquota_icms": pICMS,
        })
    return items


def parse_xml_file(path: Path) -> Dict[str, Any]:
    """
    Retorna dicionários: header, customer, items(list)
    """
    tree = ET.parse(path)
    root = tree.getroot()

    header = parse_header(root)
    cid = header.get("id_nota")
    customer = parse_customer(root, cid)
    items = parse_items(root, cid)

    return {"header": header, "customer": customer, "items": items}


# ---------- Pipeline ----------
def collect_xml_files() -> List[Path]:
    files: List[Path] = []
    for d in RAW_DIRS:
        if d.exists():
            files += sorted(d.rglob("*.xml"))
    return files

def run():
    xml_files = collect_xml_files()
    if not xml_files:
        print("[parse_xml_tiny] Nenhum XML encontrado em dados/xml_tiny/{2024,2025}.")
        sys.exit(0)

    print(f"[parse_xml_tiny] Encontrados {len(xml_files)} arquivos XML.")

    headers: List[Dict[str, Any]] = []
    customers: List[Dict[str, Any]] = []
    items_all: List[Dict[str, Any]] = []

    skipped = 0

    for fp in xml_files:
        try:
            parsed = parse_xml_file(fp)
            h = parsed["header"]
            c = parsed["customer"]
            it = parsed["items"]

            # validação mínima: precisa ter id_nota
            if not h.get("id_nota"):
                skipped += 1
                continue

            headers.append(h)
            customers.append(c)
            items_all.extend(it)

        except Exception:
            skipped += 1
            print(f"[WARN] Falha ao ler {fp.name}")
            traceback.print_exc()

    # DataFrames
    df_vendas = pd.DataFrame(headers).drop_duplicates(subset=["id_nota"])
    df_clientes = pd.DataFrame(customers).drop_duplicates(subset=["id_nota", "cpf_cnpj"])
    df_produtos = pd.DataFrame(items_all)

    # Ordenações úteis
    if "data_emissao" in df_vendas.columns:
        df_vendas["data_emissao"] = pd.to_datetime(df_vendas["data_emissao"], errors="coerce")
        df_vendas = df_vendas.sort_values("data_emissao")

    # Salvar CSVs individuais
    df_vendas.to_csv(VENDAS_CSV, index=False, encoding="utf-8")
    df_clientes.to_csv(CLIENTES_CSV, index=False, encoding="utf-8")
    df_produtos.to_csv(PRODUTOS_CSV, index=False, encoding="utf-8")

    # Merge nível item: (produtos ⟂ vendas ⟂ clientes)
    merged = df_produtos.merge(df_vendas, on="id_nota", how="left", suffixes=("", "_venda"))
    merged = merged.merge(df_clientes, on="id_nota", how="left", suffixes=("", "_cliente"))

    merged.to_csv(MERGED_CSV, index=False, encoding="utf-8")

    print(f"[parse_xml_tiny] OK!")
    print(f" - vendas:      {VENDAS_CSV}")
    print(f" - produtos:    {PRODUTOS_CSV}")
    print(f" - clientes:    {CLIENTES_CSV}")
    print(f" - tiny_merged: {MERGED_CSV}")
    if skipped:
        print(f"[parse_xml_tiny] Aviso: {skipped} arquivo(s) foram pulados por erro ou falta de id_nota.")


if __name__ == "__main__":
    run()
