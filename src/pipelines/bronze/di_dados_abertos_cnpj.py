import os, requests, zipfile
from io import BytesIO

def download_and_extract_data(end_url, destination_folder):
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

# Path relativo do projeto
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
destination_folder = os.path.join(project_root, "data", "bronze", "land_zone")

# ----------------------------------------------------------------
# Endpoint origem - Arquivos empresas
year_search="2025"
month_search="01"
file1_zip = "Empresas1.zip"

# URL para busca de arquivos RFB
end_url = f"https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/{year_search}-{month_search}/{file1_zip}"

# Invocação da função de busca e extração do endpoint
download_and_extract_data(end_url, destination_folder)

# ----------------------------------------------------------------
# Endpoint origem - Arquivos sócios
year_search = "2025"
month_search = "01"
file2_zip = "Socios1.zip"

# URL para busca de arquivos RFB
end_url = f"https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/{year_search}-{month_search}/{file2_zip}"

# Invocação da função de busca e extração do endpoint
download_and_extract_data(end_url, destination_folder)
