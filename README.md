# Portal-de-Dados-Abertos-Receita-Federal-do-Brasil
A receita federal disponibiliza alguns dados abertos sobre as empresas existentes hoje no Brasil. Esses dados são de domínio público e livre acesso, porém eles podem sofrer um desatualização de até 3 meses. Será realizada ingestão de tabelas a partir de um endpoint e posterior processamento de dados para geração de dados agregados.

Com base nos requisitos apresentados , segue definição geral a respeito da solução de arquitetura e desenvolvimento.

Medallion Architecture:
- Bronze (Ingestão de dados brutos da Receita Federal via zip em endpoint/ Tabelas Bronze)
- Silver (Normalização/ Data Cleaning/ Enrequecimento conforme regras de negócio/ Tabelas Silver)
- Gold (Dados formatados com tipagem definida para consumo por aplicações e consultas analíticas/ Tabela Gold)

Ferramentas:
- Python
- VSCode
- PySpark
- Databricks
- Docker
- Airflow
- SQL Server

Estrutura de Desenvolvimento - Pipeline de Dados:

1. Download:
- Utilizar a biblioteca requests para baixar os arquivos ZIP do endpoint.
- Extração do conteúdo dos ZIPs.
2. Camada Bronze:
- Salvar os arquivos brutos extraídos diretamente em um armazenamento.
- Armazenamento local.
3. Camada Silver:
- Processar os dados brutos e mapear para os schemas definidos:
  - Empresas: cnpj, razão_social, natureza_juridica, qualificacao_responsavel, capital_social, cod_porte.
  - Sócios: cnpj, tipo_socio, nome_socio, documento_socio, codigo_qualificacao_socio.
- Implementar validações e tratamento de erros (ex.: linhas com dados ausentes).
4. Camada Gold:
- Criar a tabela final com os campos:
  - cnpj: Identificador único da empresa.
  - qtde_socios: Número de sócios participantes.
  - flag_socio_estrangeiro: Indicador de presença de sócios estrangeiros.
  - doc_alvo: Flag indicando empresas-alvo.
- Aplicar as regras de negócio:
  - flag_socio_estrangeiro = True se documento_socio for estrangeiro.
  - doc_alvo = True quando porte da empresa = 03 e qtde_socios > 1.
