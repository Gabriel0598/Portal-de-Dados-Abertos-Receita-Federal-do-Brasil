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

# Arquitetura
Este é um escopo geral da arquitetura de dados criado através da ferramenta Excalidraw, modelo se encontra nas pastas do repositório:
![image](https://github.com/user-attachments/assets/c2bdd2c9-f19f-4428-977b-295c6b3f0cdb)

# Setup Inicial
- Inicialmente você deve instalar a versão mais recente do Python em seu OS, sendo a mais recente em Jan/2025 a versão 3.13, é possível obtê-la a partir do site oficial: (https://www.python.org/)
- Após basta abrir seu terminal na pasta do projeto, criar o ambiente venv e ativá-lo:
  - python3 -m venv .venv
  - source .venv/bin/activate
- Confirmar que está ativo:
  - which python
- Para os pacotes a serem utilizados no projeto, utilizar o gerenciador de pacotes pip:
  - python3 -m pip install --upgrade pip
  - python3 -m pip --version
- Criar e Instalar pacotes do arquivo de requirements.txt:
  - pip freeze > requirements.txt
Pacotes e versões a serem instalados já estão informados nos requirementos, apenas executar:
  - pip install -r requirements.txt
Arquivo "requirements.txt" já atende a instalação da versão certifi correta, porém caso persista em apresentar erro de certificado SSL no momento do run do python de integração, realizar upgrade manualmente do certifi através do comando:
  - pip install --upgrade certifi
Verifique o que foi instaldo:
  - pip list
Criar Databricks Workspace Premium em sua cloud de preferência e realizar os demais setups (Será necessário para executar ETL):
  - Criar Cluster Personal Compute com configuração básica para execução de jobs (DS3V2/ SingleNode/ UnityCatalog Enabled)
  - Configurar conexão VSCode e Databricks através do terminal no VS Code: databricks-connect configure
  - Seguir passo a passo de conexão informando Host, Token, Cluster ID, Org-ID (Apenas para Azure) e Port
![cli-db1](https://github.com/user-attachments/assets/4d842910-82cf-4958-b268-2cb2a4e982db)
![cli-db3](https://github.com/user-attachments/assets/b528e9bf-5360-4f24-a71e-354dc9b5ca24)

## Estrutura de pastas

![repo_structure](https://github.com/user-attachments/assets/699f48b6-8d7e-4d79-abe4-6cd511a79e64)

- architecture (diagramas de arquitetura)
- data
  - landing_zone (Dados brutos conforme origem - csv/ txt/ etc)
  - bronze (Dados Brutos como parquet - Delta Lake)
  - silver (Dados Refinados como parquet - Delta Lake)
  - gold (Dados Enriquecidos como parquet - Delta Lake)
- docker (docker-file/ Docker Compose)
- src
  - data_exporter (Exportação para banco de dados relacional/ Consultas analíticas)
  - pipelines (Códigos python/ pyspark para integração e transformação de dados, separados por camadas)
  - utils (Pacotes utilitários)
- Arquivos de configuração (.venv - ambiente python/ .gitignore/ LICENSE/ README.md/ etc)

## Execução do projeto
Bronze
- Executar "di-dados_abertos_cnpj.py" através de python "path_completo_arquivo"
- #### python "/c/Users/Proprietario/OneDrive/Documentos/Geral/Repo/Portal-de-Dados-Abertos-Receita-Federal-do-Brasil/src/pipelines/bronze/di-dados_abertos_cnpj.py"
- Para execução parcial ou complementamente com sucesso será retornado via CLI:
![code_exec](https://github.com/user-attachments/assets/759166d7-7a58-43d5-99d6-13966d8115ad)
![image](https://github.com/user-attachments/assets/047c6824-2f44-4af9-bd14-96ddb58bb2e6)

Silver
- Configurar movimentação dos arquivos locais para DBFS:
  - Executar shell localizado na pasta automation:
    - #### sh automation/databricks-configure-move-files.sh
  - Informar path completo do seu diretório local até a pasta land_zone (Caso seja Windows informar seguindo padrão com / comuns)
    - #### Ex: "C:/Users/Proprietario/OneDrive/Documentos/Geral/Repo/Portal-de-Dados-Abertos-Receita-Federal-do-Brasil/data/bronze/land_zone"
  - Após execução com sucesso arquivos serão movidos para DBFS
![cli-db4](https://github.com/user-attachments/assets/c2a4a8db-6633-4818-ab78-72f0bb9e38db)

- Executar "etl_silver_empresas.py" via Databricks
- Executar "etl_silver_socios.py" via Databricks

Gold
- Executar "etl_tabela_gold.py" via Databricks
ETL será responsável por ler diretamente as tabelas silver, realizar as devidas transformações e salvar tabela final a ser consumida pela banco de dados