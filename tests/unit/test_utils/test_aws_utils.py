"""Testes unitários para S3Handler."""

import pytest
import pandas as pd
import boto3
from io import BytesIO
from datetime import datetime
import os

from src.utils.aws_utils import S3Handler

def test_init(s3_handler):
    """Testa inicialização da classe S3Handler."""
    assert s3_handler is not None
    assert s3_handler.bucket_name == 'test-bucket'
    assert s3_handler.s3_client is not None

def test_upload_dataframe_parquet(s3_handler, sample_ipca_data):
    """Testa upload de DataFrame como Parquet."""
    # Faz upload
    success = s3_handler.upload_dataframe(
        df=sample_ipca_data,
        file_path='test/ipca',
        layer='bronze',
        format='parquet'
    )
    
    # Verificações
    assert success is True
    
    # Verifica se o arquivo existe listando arquivos
    files = s3_handler.list_files(prefix='bronze/test')
    assert len(files) == 1
    assert files[0].startswith('bronze/test/ipca_')
    assert files[0].endswith('.parquet')

def test_upload_dataframe_csv(s3_handler, sample_ipca_data):
    """Testa upload de DataFrame como CSV."""
    # Faz upload
    success = s3_handler.upload_dataframe(
        df=sample_ipca_data,
        file_path='test/ipca',
        layer='bronze',
        format='csv'
    )
    
    # Verificações
    assert success is True
    
    # Verifica se o arquivo existe listando arquivos
    files = s3_handler.list_files(prefix='bronze/test')
    assert len(files) == 1
    assert files[0].startswith('bronze/test/ipca_')
    assert files[0].endswith('.csv')

def test_download_file_parquet(s3_handler, sample_ipca_data):
    """Testa download de arquivo Parquet."""
    # Prepara: faz upload primeiro
    s3_handler.upload_dataframe(
        df=sample_ipca_data,
        file_path='test/ipca',
        layer='bronze',
        format='parquet'
    )
    
    # Lista para encontrar o arquivo
    files = s3_handler.list_files(prefix='bronze/test')
    
    # Faz download
    df = s3_handler.download_file(files[0])
    
    # Verificações
    assert df is not None
    assert len(df) == len(sample_ipca_data)
    assert set(df.columns) == set(sample_ipca_data.columns)

def test_download_file_csv(s3_handler, sample_ipca_data):
    """Testa download de arquivo CSV."""
    # Prepara: faz upload primeiro
    s3_handler.upload_dataframe(
        df=sample_ipca_data,
        file_path='test/ipca',
        layer='bronze',
        format='csv'
    )
    
    # Lista para encontrar o arquivo
    files = s3_handler.list_files(prefix='bronze/test')
    
    # Faz download
    df = s3_handler.download_file(files[0], format='csv')
    
    # Verificações
    assert df is not None
    assert len(df) == len(sample_ipca_data)

def test_list_files(s3_handler, sample_ipca_data, sample_selic_data):
    """Testa listagem de arquivos."""
    # Prepara: faz upload de múltiplos arquivos
    s3_handler.upload_dataframe(
        df=sample_ipca_data,
        file_path='test/ipca',
        layer='bronze',
        format='parquet'
    )
    
    s3_handler.upload_dataframe(
        df=sample_selic_data,
        file_path='test/selic',
        layer='bronze',
        format='parquet'
    )
    
    # Lista todos os arquivos
    all_files = s3_handler.list_files()
    assert len(all_files) == 2
    
    # Lista com prefixo
    ipca_files = s3_handler.list_files(prefix='bronze/test/ipca')
    assert len(ipca_files) == 1
    
    # Lista com outro prefixo
    selic_files = s3_handler.list_files(prefix='bronze/test/selic')
    assert len(selic_files) == 1

def test_test_connection(s3_handler):
    """Testa método de teste de conexão."""
    result = s3_handler.test_connection()
    assert result is True

def test_get_latest_file(s3_handler, sample_ipca_data):
    """Testa obtenção do arquivo mais recente."""
    # Prepara: faz upload de múltiplos arquivos com delay
    s3_handler.upload_dataframe(
        df=sample_ipca_data,
        file_path='test/ipca_v1',
        layer='bronze',
        format='parquet'
    )
    
    import time
    time.sleep(1)  # Aguarda para garantir timestamp diferente
    
    s3_handler.upload_dataframe(
        df=sample_ipca_data,
        file_path='test/ipca_v2',
        layer='bronze',
        format='parquet'
    )
    
    # Obtém o mais recente
    latest = s3_handler.get_latest_file('bronze/test')
    assert latest is not None
    assert 'ipca_v2' in latest

def test_read_write_parquet(s3_handler, sample_ipca_data):
    """Testa métodos específicos para Parquet."""
    # Testa write_parquet
    key = 'test/direct_parquet.parquet'
    success = s3_handler.write_parquet(sample_ipca_data, key)
    assert success is True
    
    # Testa read_parquet
    df = s3_handler.read_parquet(key)
    assert df is not None
    assert len(df) == len(sample_ipca_data)

def test_move_file(s3_handler, sample_ipca_data):
    """Testa movimentação de arquivos."""
    # Prepara: faz upload
    s3_handler.upload_dataframe(
        df=sample_ipca_data,
        file_path='test/source',
        layer='',  # Sem layer para simplificar o teste
        format='parquet'
    )
    
    # Lista para encontrar o arquivo
    files = s3_handler.list_files(prefix='test/source')
    
    source_path = files[0]
    dest_path = 'test/destination.parquet'
    
    # Move arquivo
    success = s3_handler.move_file(source_path, dest_path)
    assert success is True
    
    # Verifica se origem não existe mais
    files = s3_handler.list_files(prefix='test/source')
    assert len(files) == 0
    
    # Verifica se destino existe
    files = s3_handler.list_files(prefix='test/destination')
    assert len(files) == 1
    
    # Verifica conteúdo
    df = s3_handler.download_file(dest_path)
    assert df is not None
    assert len(df) == len(sample_ipca_data)