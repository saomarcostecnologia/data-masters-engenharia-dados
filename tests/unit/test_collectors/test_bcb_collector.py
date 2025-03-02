"""Testes unitários para o coletor do Banco Central do Brasil."""

import pytest
import pandas as pd
from datetime import datetime, timedelta
import requests
from unittest.mock import patch, MagicMock

from src.collectors.bcb_collector import BCBCollector

# Mock de resposta da API do BCB
@pytest.fixture
def mock_bcb_response():
    """Mock de resposta da API do BCB para o IPCA."""
    return [
        {"data": "01/01/2023", "valor": "0.53"},
        {"data": "01/02/2023", "valor": "0.84"},
        {"data": "01/03/2023", "valor": "0.71"}
    ]

def test_get_source_name(bcb_collector):
    """Testa se o nome da fonte está correto."""
    assert bcb_collector.get_source_name() == 'bcb'

def test_get_available_indicators(bcb_collector):
    """Testa se os indicadores disponíveis estão corretos."""
    indicators = bcb_collector.get_available_indicators()
    
    # Verifica se os indicadores principais estão presentes
    assert 'ipca' in indicators
    assert 'selic' in indicators
    assert 'pib' in indicators
    assert 'cambio' in indicators
    assert 'desemprego' in indicators
    
    # Verifica estrutura de um indicador
    ipca = indicators['ipca']
    assert 'code' in ipca
    assert 'name' in ipca
    assert 'unit' in ipca
    assert 'frequency' in ipca

@patch('requests.get')
def test_get_series_data_success(mock_get, bcb_collector, mock_bcb_response):
    """Testa coleta de dados com sucesso."""
    # Configura o mock
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = mock_bcb_response
    mock_get.return_value = mock_response
    
    # Coleta dados
    df = bcb_collector.get_series_data('ipca')
    
    # Verificações
    assert df is not None
    assert len(df) == 3
    assert 'data' in df.columns
    assert 'ipca' in df.columns
    assert df['ipca'].iloc[0] == 0.53
    
    # Verifica se a URL foi chamada corretamente
    url = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.433/dados"
    mock_get.assert_called_once()
    assert url in mock_get.call_args[0][0]

@patch('requests.get')
def test_get_series_data_error(mock_get, bcb_collector):
    """Testa comportamento em caso de erro na API."""
    # Configura o mock para simular erro
    mock_response = MagicMock()
    mock_response.status_code = 404
    mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError("404 Not Found")
    mock_get.return_value = mock_response
    
    # Coleta dados
    df = bcb_collector.get_series_data('ipca')
    
    # Verificações
    assert df is None

def test_post_collect_hook(bcb_collector):
    """Testa o hook de pós-coleta."""
    # Cria DataFrame básico
    df = pd.DataFrame({
        'data': pd.date_range(start='2023-01-01', periods=3, freq='MS'),
        'ipca': [0.53, 0.84, 0.71]
    })
    
    # Aplica hook
    processed_df = bcb_collector._post_collect_hook(df, 'ipca')
    
    # Verificações
    assert 'indicator' in processed_df.columns
    assert 'indicator_name' in processed_df.columns
    assert 'unit' in processed_df.columns
    assert 'frequency' in processed_df.columns
    assert 'source' in processed_df.columns
    assert 'collected_at' in processed_df.columns
    
    assert processed_df['indicator'].iloc[0] == 'ipca'
    assert processed_df['source'].iloc[0] == 'bcb'

@patch.object(BCBCollector, 'get_series_data')
@patch.object(BCBCollector, '_store_data')
def test_collect_and_store(mock_store, mock_get_series, bcb_collector, sample_ipca_data):
    """Testa o fluxo completo de coleta e armazenamento."""
    # Configura mocks
    mock_get_series.return_value = sample_ipca_data
    mock_store.return_value = True
    
    # Executa coleta
    result = bcb_collector.collect_and_store(indicators=['ipca'])
    
    # Verificações
    assert result == {'ipca': True}
    mock_get_series.assert_called_once()
    mock_store.assert_called_once()