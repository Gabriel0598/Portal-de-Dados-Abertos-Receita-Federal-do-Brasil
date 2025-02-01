import os, pytest, requests
from unittest import mock
from src.pipelines.bronze.di_dados_abertos_cnpj import download_and_extract_data

# Teste - Criação de destination folder
@pytest.fixture
def temp_directory(tmpdir):
    return str(tmpdir.mkdir("land_zone"))