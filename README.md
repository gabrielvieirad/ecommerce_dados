# Integração e Análise de Dados de E-commerce com Tiny ERP e Marketplaces

## Objetivo Geral

Este projeto tem como objetivo consolidar dados de vendas e performance de múltiplos marketplaces (Shopee, Mercado Livre, Amazon, TikTok Shop e Beleza na Web) sem o uso de APIs, utilizando apenas exportações manuais em XML e CSV/XLSX.  
O foco é oferecer uma visão unificada e confiável de faturamento, volume de vendas, ticket médio e desempenho por canal, com dashboards analíticos desenvolvidos em Power BI.

---
## Estrutura do Projeto
```bash
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

```
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
```
---


## Estrutura das Páginas do Dashboard Power BI

### Página 1 — Visão Geral
- **Gráfico de linha**: Receita mensal consolidada, exibindo a evolução temporal do faturamento total.
- **Cards principais**: Receita Total, Vendas Totais, Ticket Médio e SKUs Únicos.
- **Filtros**: Canal e Ano.
- **Objetivo**: fornecer uma visão macro do desempenho consolidado e das tendências mensais de receita.

---

### Página 2 — Comparativo de Marketplaces
- **Gráfico de colunas**: Receita Total por canal, permitindo identificar o desempenho individual de cada marketplace.
- **Gráfico de pizza**: Participação percentual de cada canal na receita total consolidada.
- **Tabela detalhada**: SKU, Produto, Canal, Vendas e Valor Total.
- **KPI adicional**: Ticket Médio comparativo entre canais.
- **Objetivo**: analisar a distribuição de receita entre marketplaces e identificar canais de maior rentabilidade.

---

### Página 3 — Top Produtos
- **Gráfico de barras**: Top 10 produtos com maior receita no período analisado.
- **Linha temporal**: evolução mensal dos produtos mais vendidos.
- **Filtros**: Canal e Ano.
- **Objetivo**: destacar os produtos de melhor desempenho e acompanhar suas variações mensais.

---

## Resultados Obtidos
- **Integração de cinco fontes de dados distintas** (Amazon, Mercado Livre, Shopee, TikTok e Beleza na Web) sem utilização de APIs.
- **Criação de uma base de dados única e padronizada** (`dados_gerais.csv`) centralizando todas as informações de vendas.
- **Correção e padronização de separadores decimais**, nomenclaturas de canais e colunas inconsistentes.
- **Implementação de medidas DAX personalizadas** para cálculo de Receita Total, Vendas Totais, Ticket Médio, SKUs Únicos e Variação Mensal.
- **Construção de relacionamento temporal com tabela calendário** para análises contínuas e comparações históricas.
- **Redução significativa do esforço manual** através de transformações automatizadas via Power Query.
- **Desenvolvimento de um dashboard analítico e interativo** com filtros dinâmicos, comparativos de marketplaces e acompanhamento de produtos estratégicos.
- **Estrutura modular e expansível**, permitindo integração de novos canais ou períodos futuros sem necessidade de refatoração completa.

---

## Considerações Técnicas
- **Tratamento de dados realizado no Power Query** com ajuste de localidade (“en-US”) para correta interpretação de valores decimais.
- **Recriação das medidas após substituição da base**, garantindo compatibilidade e consistência nos visuais.
- **Configuração de eixos contínuos e desativação de hierarquias automáticas** para melhor desempenho temporal.
- **Aplicação de tema visual** com cores padronizadas por canal e tipografia legível (Segoe UI).
- **Organização dos visuais em três páginas temáticas**, facilitando leitura executiva e análise comparativa.

---

## Aprendizados e Lições Técnicas
- **Uso de localidade no Power Query**: Foi necessário ajustar a localidade para "en-US" ao importar os dados, garantindo que os separadores decimais fossem corretamente interpretados.
- **Configuração de eixos contínuos**: Ao trabalhar com dados temporais, é fundamental configurar os eixos como contínuos para garantir que o gráfico de linha ou área mostre uma linha contínua e precisa, evitando quebras desnecessárias.
- **Utilização de tema JSON**: Aplicar um tema visual padronizado garantiu consistência no design, especialmente com o uso de cores específicas para cada canal. 
- **Escalabilidade do dashboard**: A estrutura modular do Power BI permite integrar facilmente novas fontes de dados, mantendo o dashboard funcional e expansível para futuros canais de vendas ou análises adicionais.
---

### Autor

- Gabriel Vieira
- Desenvolvedor e Analista de Dados
- Contato: www.linkedin.com/in/gabriel-vieira-6a5039117
---

### Licença

Este projeto é de uso livre para fins educacionais e demonstração técnica.
A redistribuição ou uso comercial sem autorização é proibida.

---

