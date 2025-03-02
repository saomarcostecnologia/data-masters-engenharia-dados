"""Configuração e fixtures compartilhadas para testes."""

import pytest
import os
import pandas as pd
from datetime import datetime, timedelta
import boto3
import logging
from moto import mock_s3
from io import BytesIO

from src.utils.aws_utils import S3Handler
from src.collectors.bcb_collector import BCBCollector
from src.collectors.ibge_collector import IBGECollector
from src.collectors.factory import CollectorFactory

# Desabilita logs durante testes
logging.basicConfig(level=logging.ERROR)

@pytest.fixture
def mock_environment():
    """Configura variáveis de ambiente para testes."""
    # Salva valores originais
    original_env = {}
    for key in ['AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY', 'AWS_REGION', 'AWS_BUCKET_NAME']:
        original_env[key] = os.environ.get(key)
    
    # Define valores para teste
    os.environ['AWS_ACCESS_KEY_ID'] = 'test-key'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'test-secret'
    os.environ['AWS_REGION'] = 'us-east-1'
    os.environ['AWS_BUCKET_NAME'] = 'test-bucket'
    
    yield
    
    # Restaura valores originais
    for key, value in original_env.items():
        if value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = value

@pytest.fixture
def mock_aws():
    """Mock para serviços AWS."""
    with mock_s3():
        # Cria bucket de teste
        s3_client = boto3.client(
            's3',
            region_name='us-east-1',
            aws_access_key_id='test-key',
            aws_secret_access_key='test-secret'
        )
        s3_client.create_bucket(Bucket='test-bucket')
        
        yield s3_client

@pytest.fixture
def s3_handler(mock_environment, mock_aws):
    """Fixture para S3Handler configurado para testes."""
    return S3Handler()

@pytest.fixture
def bcb_collector(mock_environment, mock_aws):
    """Coletor BCB para testes."""
    return BCBCollector()

@pytest.fixture
def ibge_collector(mock_environment, mock_aws):
    """Coletor IBGE para testes."""
    return IBGECollector()

@pytest.fixture
def collector_factory(mock_environment, mock_aws):
    """Factory de coletores para testes."""
    return CollectorFactory

# Datasets de exemplo

@pytest.fixture
def sample_ipca_data():
    """Dados de exemplo do IPCA para testes."""
    return pd.DataFrame({
        'data': pd.date_range(start='2023-01-01', periods=12, freq='MS'),
        'ipca': [0.53, 0.84, 0.71, 0.61, 0.23, 0.16, -0.38, 0.23, 0.26, 0.24, 0.28, 0.56],
        'indicator': ['ipca'] * 12,
        'indicator_name': ['IPCA - Índice Nacional de Preços ao Consumidor Amplo'] * 12,
        'unit': ['%'] * 12,
        'frequency': ['monthly'] * 12
    })

@pytest.fixture
def sample_selic_data():
    """Dados de exemplo da SELIC para testes."""
    return pd.DataFrame({
        'data': pd.date_range(start='2023-01-01', periods=12, freq='MS'),
        'selic': [13.75, 13.75, 13.75, 13.75, 13.75, 13.75, 13.25, 13.25, 12.75, 12.25, 11.75, 11.25],
        'indicator': ['selic'] * 12,
        'indicator_name': ['Taxa SELIC'] * 12,
        'unit': ['%'] * 12,
        'frequency': ['monthly'] * 12
    })

@pytest.fixture
def sample_pib_data():
    """Dados de exemplo do PIB para testes."""
    return pd.DataFrame({
        'data': pd.date_range(start='2023-01-01', periods=4, freq='QS'),
        'pib': [2.4, 0.9, 0.1, 1.2],
        'indicator': ['pib'] * 4,
        'indicator_name': ['Produto Interno Bruto'] * 4,
        'unit': ['R$ bilhões'] * 4,
        'frequency': ['quarterly'] * 4
    })

@pytest.fixture
def sample_cambio_data():
    """Dados de exemplo da taxa de câmbio para testes."""
    return pd.DataFrame({
        'data': pd.date_range(start='2023-01-01', periods=12, freq='MS'),
        'cambio': [5.28, 5.17, 5.22, 5.05, 4.98, 4.85, 4.92, 5.03, 5.17, 5.23, 5.19, 5.05],
        'indicator': ['cambio'] * 12,
        'indicator_name': ['Taxa de Câmbio (USD/BRL)'] * 12,
        'unit': ['BRL'] * 12,
        'frequency': ['monthly'] * 12
    })

@pytest.fixture
def setup_test_data(s3_handler, sample_ipca_data, sample_selic_data, sample_pib_data, sample_cambio_data):
    """Configura dados de teste no bucket."""
    # Salva dados de teste na camada bronze
    indicators = {
        'ipca': sample_ipca_data,
        'selic': sample_selic_data,
        'pib': sample_pib_data,
        'cambio': sample_cambio_data
    }
    
    for indicator, df in indicators.items():
        s3_handler.upload_dataframe(
            df=df,
            file_path=f'bcb_indicators/{indicator}',
            layer='bronze',
            format='parquet'
        )
    
    yield
    
    # Limpa dados após os testes
    files = s3_handler.list_files()
    s3_client = boto3.client(
        's3',
        region_name='us-east-1',
        aws_access_key_id='test-key',
        aws_secret_access_key='test-secret'
    )
    
    for file_path in files:
        s3_client.delete_object(Bucket='test-bucket', Key=file_path)