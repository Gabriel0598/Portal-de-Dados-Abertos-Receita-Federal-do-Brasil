# Open Data Portal - Brazilian Federal Revenue Service

The Brazilian Federal Revenue Service provides some open data about existing companies in Brazil. This data is public domain and freely accessible, but it may be up to 3 months out of date. Tables will be ingested from an endpoint and subsequently processed to generate aggregated data.

Based on the presented requirements, below is a general definition regarding the architecture and development solution.

Medallion Architecture:
- Bronze (Ingestion of raw data from the Federal Revenue Service via zip from endpoint / Bronze Tables)
- Silver (Normalization / Data Cleaning / Enrichment according to business rules / Silver Tables)
- Gold (Formatted data with defined typing for consumption by applications and analytical queries / Gold Table)

Tools:
- Python
- VSCode
- PySpark
- Databricks
- Docker
- Airflow
- SQL Server

Development Structure - Data Pipeline:

1. Download:
- Use the requests library to download ZIP files from the endpoint.
- Extract the contents of the ZIPs.
2. Bronze Layer:
- Save the extracted raw files directly to storage.
- Local storage.
3. Silver Layer:
- Process the raw data and map to the defined schemas:
  - Companies: cnpj, company_name, legal_nature, responsible_qualification, share_capital, company_size_code.
  - Partners: cnpj, partner_type, partner_name, partner_document, partner_qualification_code.
- Implement validations and error handling (e.g., rows with missing data).
4. Gold Layer:
- Create the final table with the fields:
  - cnpj: Unique company identifier.
  - qtde_socios: Number of participating partners.
  - flag_socio_estrangeiro: Indicator of the presence of foreign partners.
  - doc_alvo: Flag indicating target companies.
- Apply business rules:
  - flag_socio_estrangeiro = True if partner_document is foreign.
  - doc_alvo = True when company_size_code = 03 and qtde_socios > 1.
5. Data Catalog of Tables:
- Silver tables follow the naming convention: slv
- Gold tables follow the naming convention: gld

# Architecture

This is a general scope of the data architecture created using Excalidraw. The model can be found in the repository folders:
![image](https://github.com/user-attachments/assets/c2bdd2c9-f19f-4428-977b-295c6b3f0cdb)

# Initial Setup

- First, you must install the latest version of Python on your OS. As of Jan/2025, the latest version is 3.13, available at the official site: (https://www.python.org/)
- Then, open your terminal in the project folder, create the venv environment and activate it:
  - python3 -m venv .venv
  - source .venv/bin/activate
- Confirm it is active:
  - which python
- For the packages to be used in the project, use the pip package manager:
  - python3 -m pip install --upgrade pip
  - python3 -m pip --version
- Create and install packages from the requirements.txt file:
  - pip freeze > requirements.txt
Packages and versions to be installed are already listed in requirements, just run:
  - pip install -r requirements.txt
The "requirements.txt" file already ensures the correct certifi version is installed, but if you still get an SSL certificate error when running the integration python, manually upgrade certifi with:
  - pip install --upgrade certifi
Check what was installed:
  - pip list
Create a Databricks Premium Workspace in your preferred cloud and perform the other setups (Required to run ETL):
  - Create a Personal Compute Cluster with basic configuration for job execution (DS3V2/ SingleNode/ UnityCatalog Enabled)
  - Configure VSCode and Databricks connection via the VS Code terminal: databricks-connect configure
  - Follow the connection steps providing Host, Token, Cluster ID, Org-ID (Azure only), and Port
![cli-db1](https://github.com/user-attachments/assets/4d842910-82cf-4958-b268-2cb2a4e982db)
![cli-db3](https://github.com/user-attachments/assets/b528e9bf-5360-4f24-a71e-354dc9b5ca24)

## Folder Structure

![repo_structure](https://github.com/user-attachments/assets/699f48b6-8d7e-4d79-abe4-6cd511a79e64)

- architecture (architecture diagrams)
- data
  - landing_zone (Raw data as received - csv/ txt/ etc)
  - bronze (Raw data as parquet - Delta Lake)
  - silver (Refined data as parquet - Delta Lake)
  - gold (Enriched data as parquet - Delta Lake)
- docker (docker-file/ Docker Compose)
- src
  - data_exporter (Export to relational database / Analytical queries)
  - pipelines (Python/ PySpark code for data integration and transformation, separated by layers)
  - utils (Utility packages)
- Configuration files (.venv - python environment/ .gitignore/ LICENSE/ README.md/ etc)

## Project Execution

Databricks
- Configure and start a Databricks All-Purpose cluster of type "Standard_DS3_v2", ensure the cluster is always running to perform the transformation

Bronze
- Run "di-dados_abertos_cnpj.py" using python "full_file_path"
- #### python "/c/Users/Proprietario/OneDrive/Documentos/Geral/Repo/Portal-de-Dados-Abertos-Receita-Federal-do-Brasil/src/pipelines/bronze/di-dados_abertos_cnpj.py"
- For partial or complete execution, the result will be returned via CLI:
![code_exec](https://github.com/user-attachments/assets/759166d7-7a58-43d5-99d6-13966d8115ad)
![image](https://github.com/user-attachments/assets/047c6824-2f44-4af9-bd14-96ddb58bb2e6)

Silver
- Configure movement of local files to DBFS:
  - Run the shell script located in the automation folder:
    - #### sh automation/databricks-configure-move-files.sh
  - Provide the full path of your local directory to the land_zone folder (If using Windows, use forward slashes /)
    - #### Ex: "C:/Users/Proprietario/OneDrive/Documentos/Geral/Repo/Portal-de-Dados-Abertos-Receita-Federal-do-Brasil/data/bronze/land_zone"
  - After successful execution, files will be moved to DBFS
![cli-db4](https://github.com/user-attachments/assets/c2a4a8db-6633-4818-ab78-72f0bb9e38db)

- Run "etl_silver_empresas.py" via Databricks
- Run "etl_silver_socios.py" via Databricks

Gold
- Run "etl_tabela_gold.py" via Databricks
The ETL will read directly from the silver tables, perform the necessary transformations, and save the final table to be consumed by the database

MySQL
Run MySQL via Docker for data loading and querying

Step 1: Run Docker Compose
In the terminal, navigate to the "docker/docker-compose.yml" directory and run:

docker-compose up -d

Step 2: Run the Python script
Make sure the MySQL container is running and execute the Python script:

python src/pipelines/gold/load_data_to_mysql.py

P.S. Contact the author for information regarding the .env file
Note: Check if the MySQL service is running on your machine on port 3306 and stop it to avoid conflicts with the container