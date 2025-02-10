import os, requests, zipfile
from io import BytesIO
from datetime import datetime, timedelta

def download_and_extract_data(end_url, destination_folder, year, month):
    """
    Realiza download de arquivo zip a partir de endpoint e extrai conteúdo para diretório interno
    """
    try:
        # Verifica existencia de diretório
        if not os.path.exists(destination_folder):
            os.makedirs(destination_folder)

        # Realiza download de arquivo zip
        print(f"Download de arquivo a partir de: {end_url}")
        response = requests.get(end_url, stream=True)
        # Em caso de falha é invocada exceção
        response.raise_for_status()

        # Verifica se download foi bem sucedido e extrai os dados
        if response.status_code == 200:
            # Leitura do zip a partir da memória
            with zipfile.ZipFile(BytesIO(response.content)) as zfile:
                # Lista arquivos do zip
                file_list = zfile.namelist()
                print(f"Arquivos encontrados: {file_list}")

                print(f"Extraindo arquivos para {destination_folder}")
                zfile.extractall(destination_folder)
                print(f"Extração com sucesso para diretório: {destination_folder} ref. aos arquivos {file_list}")
                
                # Renomeia os arquivos extraídos para incluir ano e mês
                for file_name in file_list:
                    base_name, ext = os.path.splitext(file_name)
                    new_name = f"{base_name}{ext}_{year}-{month}.csv"
                    os.rename(os.path.join(destination_folder, file_name), os.path.join(destination_folder, new_name))
                    print(f"Arquivo {file_name} renomeado para {new_name}")
        elif response.status_code == 400:
            print("Requisição inválida")
        elif response.status_code == 401:
            print("Bad request")
        elif response.status_code == 403:
            print("Acesso negado")
        elif response.status_code == 404:
            print("Arquivo não encontrado no endpoint")
        else:
            print(f"Erro inesperado: HTTP {response.status_code}")
    except requests.exceptions.RequestException as e:
        print(f"Erro ao realizar download: {e}")
    except zipfile.BadZipFile:
        print("Arquivo consultado não corresponde a zip")
    except Exception as e:
        print(f"Erro inesperado: {e}")
        
def generate_date_range(start_date, end_date):
    """
    Gera uma lista de datas no formato YYYY-MM a partir de uma data inicial e uma data final
    """
    date_range = []
    current_date = start_date
    while current_date <= end_date:
        date_range.append(current_date.strftime("%Y-%m"))
        current_date += timedelta(days=32)
        current_date = current_date.replace(day=1)
    return date_range

# Path relativo do projeto
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
destination_folder = os.path.join(project_root, "data", "bronze", "land_zone")

# ----------------------------------------------------------------
# Endpoint origem - Arquivos empresas
# Input de range de datas pelo usuário
print("Consultar arquivos do enpoint por período")
print("É recomendável selecionar um período superior a três meses devido a variação de registros neste período\n")

start_year = input("Informe o ano inicial (YYYY) para consulta dos arquivos: ")
start_month = input("Informe o mês inicial (MM) para consulta dos arquivos: ")
end_year = input("Informe o ano final (YYYY) para consulta dos arquivos: ")
end_month = input("Informe o mês final (MM) para consulta dos arquivos: ")

# Arquivos
file_zip_emp = "Empresas0.zip"
file_zip_soc = "Socios0.zip"

# Converte datas de entrada
start_date = datetime.strptime(f"{start_year}-{start_month}", "%Y-%m")
end_date = datetime.strptime(f"{end_year}-{end_month}", "%Y-%m")

# Gerar lista de datas para consulta
date_range = generate_date_range(start_date, end_date)

# Invocação da função de busca e extração do endpoint
for date in date_range:
    year_search, month_search = date.split("-")
    # Extração arquivo empresas
    end_url = f"https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/{year_search}-{month_search}/{file_zip_emp}"
    download_and_extract_data(end_url, destination_folder, year_search, month_search)
    # Extração arquivo socios
    end_url = f"https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/{year_search}-{month_search}/{file_zip_soc}"
    download_and_extract_data(end_url, destination_folder, year_search, month_search)