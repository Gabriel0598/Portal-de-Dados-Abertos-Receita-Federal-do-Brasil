#! bin/bash

# read -p "Informe seu diretório local para acesso aos arquivos bronze/ land zone (path completo): " LOCAL_DIR

# databricks configure --token
# databricks fs mkdirs dbfs:/FileStore/shared_uploads/default_user

# DBFS_DIR=dbfs:/FileStore/shared_uploads/default_user
# databricks fs cp --recursive $LOCAL_DIR $DBFS_DIR#!/bin/bash

read -p "Informe seu diretório local para acesso aos arquivos bronze/ land zone (path completo): " LOCAL_DIR

# Verifica se o arquivo de configuração do Databricks já existe
CONFIG_FILE="$HOME/.databrickscfg"
if [ ! -f "$CONFIG_FILE" ]; then
    echo "Arquivo de configuração do Databricks não encontrado. Configurando agora..."
    databricks configure --token
else
    echo "Arquivo de configuração do Databricks encontrado. Pulando configuração..."
fi

DBFS_DIR=dbfs:/FileStore/shared_uploads/default_user/bronze

# Verifica se o diretório existe antes de criar
if ! databricks fs ls $DBFS_DIR > /dev/null 2>&1; then
    echo "Diretório $DBFS_DIR não encontrado. Criando agora..."
    databricks fs mkdirs $DBFS_DIR
    echo "Diretório $DBFS_DIR criado com sucesso."
else
    echo "Diretório $DBFS_DIR já existe."
fi

# Verifica se o diretório local existe
if [ ! -d "$LOCAL_DIR" ]; then
    echo "Erro: O diretório local $LOCAL_DIR não existe."
    exit 1
else
    echo "Diretório local $LOCAL_DIR encontrado."
fi

# Lista os arquivos no diretório local para depuração
echo "Arquivos no diretório local $LOCAL_DIR:"
ls -l "$LOCAL_DIR"

# Copia todos os arquivos da pasta local para o DBFS
echo "Copiando arquivos de $LOCAL_DIR para $DBFS_DIR..."
databricks fs cp --recursive "$LOCAL_DIR" "$DBFS_DIR"
