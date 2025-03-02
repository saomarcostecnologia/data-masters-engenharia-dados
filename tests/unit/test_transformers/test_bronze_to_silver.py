"""Testes unitários para transformador Bronze para Silver."""

import pytest
import pandas as pd
from datetime import datetime
from unittest.mock import patch, MagicMock

from src.transformers.bronze_to_silver import EconomicIndicatorTransformer
from src.utils.aws_utils import S3Handler

def test_transform_ipca():
    """Testa transformação do IPCA."""
    # Cria o transformador
    transformer = EconomicIndicatorTransformer()
    
    # Cria dados de entrada
    input_df = pd.DataFrame({
        'data': pd.date_range(start='2023-01-01', periods=12, freq='MS'),
        'ipca': [0.53, 0.84, 0.71, 0.61, 0.23, 0.16, -0.38, 0.23, 0.26, 0.24, 0.28, 0.56],
        'indicator': ['ipca'] * 12,
        'indicator_name': ['IPCA - Índice Nacional de Preços ao Consumidor Amplo'] * 12,
        'unit': ['%'] * 12,
        'frequency': ['monthly'] * 12
    })
    
    # Executa transformação
    result_df = transformer.transform_ipca(input_df)
    
    # Verificações
    assert result_df is not None
    assert 'date' in result_df.columns
    assert 'value' in result_df.columns
    assert 'monthly_change_pct' in result_df.columns
    assert 'year_over_year_pct' in result_df.columns
    assert 'moving_avg_3m' in result_df.columns
    
    # Verifica cálculos
    assert result_df['value'].equals(input_df['ipca'])
    assert len(result_df) == len(input_df)

def test_transform_selic():
    """Testa transformação da SELIC."""
    # Cria o transformador
    transformer = EconomicIndicatorTransformer()
    
    # Cria dados diários de entrada
    dates = pd.date_range(start='2023-01-01', periods=60, freq='D')
    values = [13.75] * 30 + [13.25] * 30
    
    input_df = pd.DataFrame({
        'data': dates,
        'selic': values,
        'indicator': ['selic'] * 60,
        'indicator_name': ['Taxa SELIC'] * 60,
        'unit': ['%'] * 60,
        'frequency': ['daily'] * 60
    })
    
    # Executa transformação
    result_df = transformer.transform_selic(input_df)
    
    # Verificações
    assert result_df is not None
    assert 'date' in result_df.columns
    assert 'value' in result_df.columns
    
    # Verifica que os dados diários foram agregados para mensais
    assert len(result_df) < len(input_df)
    # Deve haver no máximo 2 meses nos dados
    assert len(result_df) <= 2

def test_transform_cambio():
    """Testa transformação da taxa de câmbio."""
    # Cria o transformador
    transformer = EconomicIndicatorTransformer()
    
    # Cria dados de entrada
    input_df = pd.DataFrame({
        'data': pd.date_range(start='2023-01-01', periods=12, freq='MS'),
        'cambio': [5.28, 5.17, 5.22, 5.05, 4.98, 4.85, 4.92, 5.03, 5.17, 5.23, 5.19, 5.05],
        'indicator': ['cambio'] * 12,
        'indicator_name': ['Taxa de Câmbio (USD/BRL)'] * 12,
        'unit': ['BRL'] * 12,
        'frequency': ['monthly'] * 12
    })
    
    # Executa transformação
    result_df = transformer.transform_cambio(input_df)
    
    # Verificações
    assert result_df is not None
    assert 'open' in result_df.columns
    assert 'close' in result_df.columns
    assert 'high' in result_df.columns
    assert 'low' in result_df.columns
    assert 'value' in result_df.columns  # Valor padronizado
    assert 'volatility' in result_df.columns
    
    # Verifica que o comprimento é mantido
    assert len(result_df) == len(input_df)

@patch.object(S3Handler, 'list_files')
@patch.object(S3Handler, 'download_file')
@patch.object(S3Handler, 'upload_dataframe')
def test_process_indicator(mock_upload, mock_download, mock_list, sample_ipca_data):
    """Testa o fluxo completo de processamento de um indicador."""
    # Configura mocks
    mock_list.return_value = ['bronze/bcb_indicators/ipca_20230101.parquet']
    mock_download.return_value = sample_ipca_data
    mock_upload.return_value = True
    
    # Cria o transformador
    transformer = EconomicIndicatorTransformer()
    
    # Processa o indicador
    result = transformer.process_indicator('ipca')
    
    # Verificações
    assert result is True
    mock_list.assert_called_once()
    mock_download.assert_called_once()
    mock_upload.assert_called_once()