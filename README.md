# Integração e Análise de Dados de E-commerce com Tiny ERP e Marketplaces

## Objetivo Geral

Este projeto tem como objetivo consolidar dados de vendas e performance de múltiplos marketplaces (Shopee, Mercado Livre, Amazon, TikTok Shop e Beleza na Web) sem o uso de APIs, utilizando apenas exportações manuais em XML e CSV/XLSX.  
O foco é oferecer uma visão unificada e confiável de faturamento, volume de vendas, ticket médio e desempenho por canal, com dashboards analíticos desenvolvidos em Power BI.

---

## Estrutura do Projeto

projeto_ecommerce_dados/
│
├── dados/
│ ├── xml_tiny/ # XMLs do Tiny ERP (base financeira)
│ ├── csv_marketplaces/ # Arquivos brutos dos marketplaces
│ │ ├── shopee/
│ │ ├── mercadolivre.xlsx
│ │ ├── amazon.csv
│ │ ├── tiktok/
│ │ └── blz.xlsx
│ └── custos_internos/
│
├── processados/
│ ├── tiny_data.db # Banco SQLite consolidado
│ ├── padronizados/ # Bases tratadas e normalizadas
│ └── dados_gerais.csv # Base final unificada (para Power BI)
│
├── scripts/
│ ├── parse_xml_tiny.py # Extrai e estrutura XMLs do Tiny ERP
│ ├── parse_xml_tiktok.py # Extrai e consolida XMLs do TikTok
│ ├── merge_shopee.py # Consolida pastas mensais da Shopee
│ ├── merge_meli.py # Trata e limpa dados do Mercado Livre
│ ├── merge_csv_marketplaces.py # Une todas as fontes ao Tiny ERP
│ └── padronizador_final.py # Padroniza e unifica todas as bases
│
├── dashboards/
│ ├── powerbi/
│ │ └── dashboard_ecommerce.pbix
│
└── README.md


---

## Stack Técnica

- Linguagem: Python 3.10+
- Bibliotecas: `pandas`, `numpy`, `matplotlib`, `openpyxl`, `xml.etree`, `sqlite3`
- Banco de Dados: SQLite (tiny_data.db)
- Ambientes: VS Code (produção) e Google Colab (análise exploratória)
- Visualização: Power BI

---

## Etapas do Projeto

### Fase 1 — Estrutura e Preparação
- Organização da estrutura de diretórios.
- Criação do ambiente virtual e instalação das dependências.

### Fase 2 — Extração (Tiny ERP)
- Leitura dos XMLs do Tiny ERP.
- Extração e consolidação de:
  - vendas.csv  
  - produtos.csv  
  - clientes.csv
- Criação do banco SQLite (`tiny_data.db`).

### Fase 3 — Consolidação (Marketplaces)
- Leitura e limpeza dos arquivos de marketplaces:
  - Shopee (arquivos mensais)
  - Mercado Livre
  - Amazon
  - TikTok Shop
  - Beleza na Web
- Padronização das colunas:  
  `sku`, `produto`, `ano`, `mes`, `canal`, `vendas`, `valor_total`
- Agrupamento por SKU, mês e ano.

### Fase 4 — Padronização Final
- Arredondamento de valores (duas casas decimais).
- Remoção de registros inválidos (NaN ou "Revisar").
- Geração do arquivo final unificado:  
  `processados/dados_gerais.csv`

### Fase 5 — Análise e Dashboard (Power BI)
- Importação do `dados_gerais.csv` para o Power BI.
- Criação de medidas DAX:

```DAX
Vendas Totais = SUM('dados_gerais'[vendas])
Receita Total = SUM('dados_gerais'[valor_total])
Ticket Médio = DIVIDE([Receita Total], [Vendas Totais])
SKUs Únicos = DISTINCTCOUNT('dados_gerais'[sku])

---

Estrutura das Páginas do Dashboard Power BI
Página 1 — Visão Geral

Gráfico de área ou linha: Receita mensal consolidada.

Cards: Receita Total, Vendas Totais, Ticket Médio.

Filtros: Canal e Ano.

Página 2 — Comparativo de Marketplaces

Gráfico de colunas: Receita por Canal.

Gráfico de pizza: Participação percentual por canal.

Tabela detalhada com SKU, Produto, Canal, Vendas e Valor Total.

Página 3 — Top Produtos

Gráfico de barras: Top 10 produtos por receita.

Linha temporal mostrando evolução mensal dos produtos mais vendidos.

Filtros de Canal e Ano.

Resultados Obtidos

Integração de cinco fontes distintas sem uso de APIs.

Criação de uma base única e padronizada (dados_gerais.csv).

Redução significativa de esforço manual com scripts de automação.

Dashboard analítico interativo com comparativo completo de marketplaces.

Pipeline modular e expansível para futuras integrações.

Próximos Passos

Implementar atualização automática mensal.

Incluir previsões de vendas (Prophet/ARIMA).

Criar alertas automáticos de performance.

Integrar o pipeline com o Power BI Service.

Autor

Gabriel Vieira
Desenvolvedor e Analista de Dados
Contato: [seu e-mail ou LinkedIn]

Licença

Este projeto é de uso livre para fins educacionais e demonstração técnica.
A redistribuição ou uso comercial sem autorização é proibida.


---

Se quiser, posso gerar o arquivo `.md` real para você baixar e subir diretamente no GitHub (sem precisar copiar/colar manualmente). Deseja que eu gere o arquivo README.md para download?
