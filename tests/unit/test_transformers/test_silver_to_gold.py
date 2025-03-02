"""Testes unitários para o transformador Silver para Gold."""

import pytest
import pandas as pd
from datetime import datetime
from unittest.mock import patch, MagicMock

from src.transformers.silver_to_gold import EconomicIndicatorsGoldTransformer
from src.utils.aws_utils import S3Handler

@pytest.fixture
def silver_ipca_data():
    """Dados simulados da camada silver para o IPCA."""
    return pd.DataFrame({
        'date': pd.date_range(start='2023-01-01', periods=12, freq='MS'),
        'value': [0.53, 0.84, 0.71, 0.61, 0.23, 0.16, -0.38, 0.23, 0.26, 0.24, 0.28, 0.56],
        'monthly_change_pct': [0.1, 0.31, -0.13, -0.1, -0.38, -0.07, -0.54, 0.61, 0.03, -0.02, 0.04, 0.28],
        'year_over_year_pct': [5.77, 5.6, 4.65, 4.18, 3.94, 3.16, 3.16, 3.43, 3.61, 4.82, 4.68, 4.62],
        'indicator': ['ipca'] * 12,
        'indicator_name': ['IPCA - Índice Nacional de Preços ao Consumidor Amplo'] * 12,
        'unit': ['%'] * 12,
        'frequency': ['monthly'] * 12
    })

@pytest.fixture
def silver_selic_data():
    """Dados simulados da camada silver para a SELIC."""
    return pd.DataFrame({
        'date': pd.date_range(start='2023-01-01', periods=12, freq='MS'),
        'value': [13.75, 13.75, 13.75, 13.75, 13.75, 13.75, 13.25, 13.25, 12.75, 12.25, 11.75, 11.25],
        'change_bps': [0, 0, 0, 0, 0, 0, -50, 0, -50, -50, -50, -50],
        'moving_avg_3m': [13.75, 13.75, 13.75, 13.75, 13.75, 13.75, 13.58, 13.42, 13.08, 12.75, 12.25, 11.75],
        'indicator': ['selic'] * 12,
        'indicator_name': ['Taxa SELIC'] * 12,
        'unit': ['%'] * 12,
        'frequency': ['monthly'] * 12
    })

@pytest.fixture
def silver_desemprego_data():
    """Dados simulados da camada silver para o desemprego."""
    return pd.DataFrame({
        'date': pd.date_range(start='2023-01-01', periods=4, freq='QS'),
        'value': [8.8, 8.3, 7.9, 7.5],
        'quarterly_change_pp': [-0.4, -0.5, -0.4, -0.4],
        'annual_change_pp': [-2.1, -1.8, -1.3, -1.0],
        'indicator': ['desemprego'] * 4,
        'indicator_name': ['Taxa de Desemprego'] * 4,
        'unit': ['%'] * 4,
        'frequency': ['quarterly'] * 4
    })

def test_normalize_date_column():
    """Testa normalização de colunas de data."""
    # Cria transformador
    transformer = EconomicIndicatorsGoldTransformer()
    
    # Cria DataFrame com tipos de data variados
    df = pd.DataFrame({
        'last_date': ['2023-01-01', pd.Timestamp('2023-02-01'), '2023-03-01T00:00:00'],
        'value': [1, 2, 3]
    })
    
    # Aplica normalização
    result = transformer.normalize_date_column(df)
    
    # Verifica se todas as datas foram convertidas para datetime
    assert pd.api.types.is_datetime64_any_dtype(result['last_date'])
    assert len(result) == 3

@patch.object(S3Handler, 'list_files')
@patch.object(S3Handler, 'download_file')
def test_load_latest_indicators(mock_download, mock_list):
    """Testa carregamento dos indicadores mais recentes."""
    # Cria transformador
    transformer = EconomicIndicatorsGoldTransformer()
    
    # Configura mocks
    mock_list.side_effect = lambda prefix: (
        ['silver/economic_indicators/ipca_20230101.parquet'] if 'ipca' in prefix
        else ['silver/economic_indicators/selic_20230101.parquet'] if 'selic' in prefix
        else []
    )
    
    # Cria dados mock para cada indicador
    ipca_data = pd.DataFrame({'date': ['2023-01-01'], 'value': [0.5], 'indicator': ['ipca']})
    selic_data = pd.DataFrame({'date': ['2023-01-01'], 'value': [13.75], 'indicator': ['selic']})
    
    mock_download.side_effect = lambda path: (
        ipca_data if 'ipca' in path
        else selic_data if 'selic' in path
        else None
    )
    
    # Executa a função
    indicators = transformer.load_latest_indicators()
    
    # Verificações
    assert 'ipca' in indicators
    assert 'selic' in indicators
    assert indicators['ipca'] is not None
    assert indicators['selic'] is not None
    assert mock_list.call_count >= 2
    assert mock_download.call_count >= 2

def test_create_monthly_indicators(silver_ipca_data, silver_selic_data):
    """Testa criação do painel mensal de indicadores."""
    # Cria transformador
    transformer = EconomicIndicatorsGoldTransformer()
    
    # Prepara indicadores
    indicators = {
        'ipca': silver_ipca_data,
        'selic': silver_selic_data
    }
    
    # Executa a função
    monthly_panel = transformer.create_monthly_indicators(indicators)
    
    # Verificações
    assert monthly_panel is not None
    assert len(monthly_panel) > 0
    assert 'year_month' in monthly_panel.columns
    assert 'ipca' in monthly_panel.columns
    assert 'selic' in monthly_panel.columns
    
    # Verifica cálculo da taxa real de juros
    assert 'real_interest_rate' in monthly_panel.columns
    
    # Verifica se o painel está ordenado
    assert monthly_panel['year_month'].is_monotonic_increasing

def test_create_labor_market_indicators(silver_desemprego_data):
    """Testa criação do painel do mercado de trabalho."""
    # Cria transformador
    transformer = EconomicIndicatorsGoldTransformer()
    
    # Prepara indicadores
    indicators = {
        'desemprego': silver_desemprego_data
    }
    
    # Executa a função
    labor_panel = transformer.create_labor_market_indicators(indicators)
    
    # Verificações
    assert labor_panel is not None
    assert len(labor_panel) > 0
    assert 'unemployment_rate' in labor_panel.columns or 'value' in labor_panel.columns
    
    # Verifica se contém colunas de variação
    variation_columns = [col for col in labor_panel.columns if 'change' in col.lower()]
    assert len(variation_columns) > 0

def test_create_macro_dashboard(silver_ipca_data, silver_selic_data, silver_desemprego_data):
    """Testa criação do dashboard macroeconômico."""
    # Cria transformador
    transformer = EconomicIndicatorsGoldTransformer()
    
    # Prepara indicadores
    indicators = {
        'ipca': silver_ipca_data,
        'selic': silver_selic_data,
        'desemprego': silver_desemprego_data
    }
    
    # Executa a função
    macro_dashboard = transformer.create_macro_dashboard(indicators)
    
    # Verificações
    assert macro_dashboard is not None
    assert len(macro_dashboard) > 0
    assert 'indicator' in macro_dashboard.columns
    assert 'indicator_name' in macro_dashboard.columns
    assert 'last_value' in macro_dashboard.columns
    assert 'last_date' in macro_dashboard.columns
    
    # Verifica presença dos indicadores principais
    indicators_present = macro_dashboard['indicator'].unique()
    assert 'ipca' in indicators_present
    assert 'selic' in indicators_present
    
    # Verifica se o índice de saúde econômica foi calculado
    assert 'economic_health' in indicators_present

@patch.object(S3Handler, 'upload_dataframe')
def test_save_to_gold_layer(mock_upload, silver_ipca_data):
    """Testa o salvamento na camada gold."""
    # Cria transformador
    transformer = EconomicIndicatorsGoldTransformer()
    
    # Configura mock
    mock_upload.return_value = True
    
    # Executa a função
    success = transformer.save_to_gold_layer(silver_ipca_data, "monthly_indicators")
    
    # Verificações
    assert success is True
    mock_upload.assert_called_once()
    
    # Verifica parâmetros da chamada
    args, kwargs = mock_upload.call_args
    assert kwargs['layer'] == 'gold/dashboards'
    assert 'monthly_indicators' in kwargs['file_path']

@patch.object(EconomicIndicatorsGoldTransformer, 'load_latest_indicators')
@patch.object(EconomicIndicatorsGoldTransformer, 'create_monthly_indicators')
@patch.object(EconomicIndicatorsGoldTransformer, 'create_labor_market_indicators')
@patch.object(EconomicIndicatorsGoldTransformer, 'create_macro_dashboard')
@patch.object(EconomicIndicatorsGoldTransformer, 'save_to_gold_layer')
def test_process_gold_layer(mock_save, mock_create_macro, mock_create_labor, 
                           mock_create_monthly, mock_load):
    """Testa o fluxo completo de processamento da camada gold."""
    # Cria transformador
    transformer = EconomicIndicatorsGoldTransformer()
    
    # Configura mocks
    indicators = {
        'ipca': pd.DataFrame({'date': ['2023-01-01'], 'value': [0.5]}),
        'selic': pd.DataFrame({'date': ['2023-01-01'], 'value': [13.75]})
    }
    mock_load.return_value = indicators
    
    monthly_panel = pd.DataFrame({'year_month': ['2023-01'], 'ipca': [0.5], 'selic': [13.75]})
    mock_create_monthly.return_value = monthly_panel
    
    labor_panel = pd.DataFrame({'date': ['2023-01-01'], 'unemployment_rate': [8.5]})
    mock_create_labor.return_value = labor_panel
    
    macro_dashboard = pd.DataFrame({'indicator': ['ipca', 'selic'], 'last_value': [0.5, 13.75]})
    mock_create_macro.return_value = macro_dashboard
    
    mock_save.return_value = True
    
    # Executa a função
    result = transformer.process_gold_layer()
    
    # Verificações
    assert result is True
    mock_load.assert_called_once()
    mock_create_monthly.assert_called_once()
    mock_create_labor.assert_called_once()
    mock_create_macro.assert_called_once()
    assert mock_save.call_count == 3  # Um para cada dashboard