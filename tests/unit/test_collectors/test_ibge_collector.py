"""Testes unitários para o coletor de dados do IBGE."""

import pytest
import pandas as pd
from datetime import datetime, timedelta
import requests
from unittest.mock import patch, MagicMock

from src.collectors.ibge_collector import IBGECollector

# Mock de resposta da API do IBGE para diferentes endpoints
@pytest.fixture
def mock_ibge_sidra_response():
    """Mock de resposta da API SIDRA do IBGE para o IPCA-15."""
    return [
        {
            "id": "7062",
            "variavel": "IPCA-15",
            "unidade": "variação percentual",
            "resultados": [
                {
                    "classificacoes": [],
                    "series": [
                        {
                            "localidade": {"nivel": {"id": "N1"}, "id": "1", "nome": "Brasil"},
                            "serie": {
                                "202201": {"data": "01/2022", "valor": "0.58"},
                                "202202": {"data": "02/2022", "valor": "0.99"},
                                "202203": {"data": "03/2022", "valor": "0.95"}
                            }
                        }
                    ]
                }
            ]
        }
    ]

@pytest.fixture
def mock_ibge_pnad_periods():
    """Mock de resposta da API PNAD para os períodos disponíveis."""
    return [
        {"id": "202301", "nome": "1º trimestre 2023"},
        {"id": "202304", "nome": "2º trimestre 2023"},
        {"id": "202307", "nome": "3º trimestre 2023"}
    ]

@pytest.fixture
def mock_ibge_pnad_data():
    """Mock de resposta da API PNAD para os dados de desemprego."""
    return [
        {
            "id": "4099",
            "variavel": "Taxa de desocupação",
            "unidade": "%",
            "resultados": [
                {
                    "series": [
                        {
                            "localidade": {"id": "1", "nome": "Brasil"},
                            "serie": [{"valor": "8.8"}]
                        }
                    ]
                }
            ]
        }
    ]

def test_get_source_name(ibge_collector):
    """Testa se o nome da fonte está correto."""
    assert ibge_collector.get_source_name() == 'ibge'

def test_get_available_indicators(ibge_collector):
    """Testa se os indicadores disponíveis estão corretos."""
    indicators = ibge_collector.get_available_indicators()
    
    # Verifica se os indicadores principais estão presentes
    assert 'ipca15' in indicators
    assert 'inpc' in indicators
    assert 'pnad' in indicators
    assert 'pib_ibge' in indicators
    
    # Verifica estrutura do indicador IPCA-15
    ipca15 = indicators['ipca15']
    assert 'code' in ipca15
    assert 'name' in ipca15
    assert 'unit' in ipca15
    assert 'frequency' in ipca15
    assert 'variables' in ipca15
    assert 'classifications' in ipca15
    
    # Verifica estrutura do indicador PNAD
    pnad = indicators['pnad']
    assert 'code' in pnad
    assert 'name' in pnad
    assert 'unit' in pnad
    assert 'frequency' in pnad
    assert 'type' in pnad
    assert pnad['type'] == 'special'  # Deve usar endpoint especial

@patch('requests.get')
def test_get_sidra_data(mock_get, ibge_collector, mock_ibge_sidra_response):
    """Testa coleta de dados via API SIDRA."""
    # Configura o mock
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = mock_ibge_sidra_response
    mock_get.return_value = mock_response
    
    # Acessa método interno _get_sidra_data
    # Nota: Em testes reais, seria melhor usar get_series_data, mas para isolar melhor usamos o método interno
    indicator_config = ibge_collector.get_available_indicators()['ipca15']
    df = ibge_collector._get_sidra_data('ipca15', indicator_config)
    
    # Verificações
    assert df is not None
    assert len(df) > 0
    assert 'data' in df.columns
    assert 'valor' in df.columns or 'value' in df.columns
    
    # Verifica se a URL foi chamada
    mock_get.assert_called_once()

@patch('requests.get')
def test_get_pnad_data(mock_get, ibge_collector, mock_ibge_pnad_periods, mock_ibge_pnad_data):
    """Testa coleta de dados da PNAD."""
    # Configura mocks para diferentes chamadas
    mock_periods_response = MagicMock()
    mock_periods_response.status_code = 200
    mock_periods_response.json.return_value = mock_ibge_pnad_periods
    
    mock_data_response = MagicMock()
    mock_data_response.status_code = 200
    mock_data_response.json.return_value = mock_ibge_pnad_data
    
    # Configura o mock.get para retornar diferentes respostas dependendo da URL
    mock_get.side_effect = lambda url, **kwargs: (
        mock_periods_response if "periodos" in url and "/periodos/" not in url
        else mock_data_response
    )
    
    # Acessa método interno _get_pnad_data
    df = ibge_collector._get_pnad_data()
    
    # Verificações
    assert df is not None
    assert len(df) > 0
    assert 'data' in df.columns
    assert 'pnad' in df.columns
    
    # Verifica se a URL foi chamada pelo menos duas vezes (períodos e dados)
    assert mock_get.call_count >= 2

@patch.object(IBGECollector, '_get_sidra_data')
@patch.object(IBGECollector, '_get_pnad_data')
def test_get_series_data(mock_get_pnad, mock_get_sidra, ibge_collector):
    """Testa o método principal get_series_data."""
    # Configura mocks
    mock_get_sidra.return_value = pd.DataFrame({
        'data': pd.date_range(start='2023-01-01', periods=3, freq='MS'),
        'valor': [0.5, 0.7, 0.3]
    })
    
    mock_get_pnad.return_value = pd.DataFrame({
        'data': pd.date_range(start='2023-01-01', periods=3, freq='QS'),
        'pnad': [8.8, 8.5, 8.1]
    })
    
    # Testa para IPCA-15 (usa SIDRA)
    df_ipca = ibge_collector.get_series_data('ipca15')
    assert df_ipca is not None
    assert len(df_ipca) == 3
    mock_get_sidra.assert_called_once()
    
    # Reset mock
    mock_get_sidra.reset_mock()
    
    # Testa para PNAD (usa endpoint especial)
    df_pnad = ibge_collector.get_series_data('pnad')
    assert df_pnad is not None
    assert len(df_pnad) == 3
    mock_get_pnad.assert_called_once()

def test_post_collect_hook(ibge_collector):
    """Testa o hook de pós-coleta."""
    # Cria DataFrame básico
    df = pd.DataFrame({
        'data': pd.date_range(start='2023-01-01', periods=3, freq='MS'),
        'value': [0.53, 0.84, 0.71]
    })
    
    # Aplica hook
    processed_df = ibge_collector._post_collect_hook(df, 'ipca15')
    
    # Verificações
    assert 'indicator' in processed_df.columns
    assert 'indicator_name' in processed_df.columns
    assert 'unit' in processed_df.columns
    assert 'frequency' in processed_df.columns
    assert 'source' in processed_df.columns
    assert 'collected_at' in processed_df.columns
    
    assert processed_df['indicator'].iloc[0] == 'ipca15'
    assert processed_df['source'].iloc[0] == 'ibge'

@patch.object(IBGECollector, 'get_series_data')
@patch.object(IBGECollector, '_store_data')
def test_collect_and_store(mock_store, mock_get_series, ibge_collector):
    """Testa o fluxo completo de coleta e armazenamento."""
    # Configura dados de teste
    sample_data = pd.DataFrame({
        'data': pd.date_range(start='2023-01-01', periods=3, freq='MS'),
        'ipca15': [0.5, 0.7, 0.3],
        'indicator': ['ipca15'] * 3,
        'indicator_name': ['IPCA-15'] * 3,
        'unit': ['%'] * 3,
        'frequency': ['monthly'] * 3
    })
    
    # Configura mocks
    mock_get_series.return_value = sample_data
    mock_store.return_value = True
    
    # Executa coleta
    result = ibge_collector.collect_and_store(indicators=['ipca15'])
    
    # Verificações
    assert result == {'ipca15': True}
    mock_get_series.assert_called_once()
    mock_store.assert_called_once()