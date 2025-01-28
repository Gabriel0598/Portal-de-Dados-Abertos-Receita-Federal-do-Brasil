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

        # Leitura do zip a partir da memória
        with zipfile.ZipFile(BytesIO(response.content)) as zfile:
            # Lista arquivos do zip
            file_list = zfile.namelist()
            print(f"Arquivos encontrados: {file_list}")

            print(f"Extraindo arquivos para {destination_folder}")
            zfile.extractall(destination_folder)
            print("Extração com sucesso")
    except requests.exceptions.RequestException as e:
        print(f"Erro ao realizar download: {e}")
    except zipfile.BadZipFile:
        print("Arquivo baixado não corresponde a zip")
    except Exception as e:
        print(f"Erro inesperado: {e}")

# Endpoint origem - Arquivos empresas
year_search="2025"
month_search="01"
file_zip = "Empresas1.zip"

# URL para busca de arquivos especificos
end_url = f"https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/{year_search}-{month_search}/{file_zip}"

# Diretório destino
destination_folder = "/data/bronze/land_zone"

# Invocação da função de busca e extração do endpoint
download_and_extract_data(end_url, destination_folder)
