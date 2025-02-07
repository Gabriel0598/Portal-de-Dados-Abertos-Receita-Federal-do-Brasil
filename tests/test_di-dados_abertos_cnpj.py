import sys, os, pytest, requests
from unittest.mock import patch, Mock
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from src.pipelines.bronze.di_dados_abertos_cnpj import download_and_extract_data

# Endpoint origem - Arquivos empresas
# Alterar file1_zip conforme a necessidade de cada teste
year_search="2025"
month_search="01"
file1_zip = "Empresas1.zip"

# URL para busca de arquivos RFB
end_url = f"https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/{year_search}-{month_search}/{file1_zip}"

# Teste - Criação de destination folder
@pytest.fixture
def temp_directory(tmpdir):
    return str(tmpdir.mkdir("land_zone"))

@patch('requests.get')
def test_download_and_extract_data(mock_get, temp_directory):
    zip_content = end_url
    mock_get.return_value = Mock(status_code=200, content=zip_content)

    with patch("zipfile.ZipFile") as mock_zip:
        mock_zip.return_value.namelist.return_value = ["file1.txt"]
        mock_zip.return_value.extractall.return_value = None

        download_and_extract_data(end_url, temp_directory)

        assert os.path.exists(temp_directory)
        mock_zip.return_value.extractall.assert_called_once_with(temp_directory)

@patch("requests.get")
def test_http_error_404(mock_get, temp_directory):
    mock_get.return_value = Mock(status_code=404)

    with patch("builtins.print") as mock_print:
        download_and_extract_data(end_url, temp_directory)
        mock_print.assert_any_call("Arquivo não encontrado no endpoint")

@patch("requests.get")
def test_invalid_zip_file(mock_get, temp_directory):
    mock_get.return_value = Mock(status_code=200, content=b"Not a zip file")

    with patch("builtins.print") as mock_print:
        download_and_extract_data(end_url, temp_directory)
        mock_print.assert_any_call("Arquivo consultado não corresponde a zip")

@patch("requests.get", side_effect=requests.exceptions.RequestException("Erro de conexão"))
def test_request_exception(mock_get, temp_directory):
    with patch("builtins.print") as mock_print:
        download_and_extract_data(end_url, temp_directory)
        mock_print.assert_any_call("Erro ao realizar download: Erro de conexão")