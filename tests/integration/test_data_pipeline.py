"""Testes de integração para o pipeline de dados completo."""

import pytest
import os
import pandas as pd
from datetime import datetime, timedelta
import logging
from unittest.mock import patch, MagicMock

from src.collectors.bcb_collector import BCBCollector
from src.transformers.bronze_to_silver import EconomicIndicatorTransformer
from src.transformers.silver_to_gold import EconomicIndicatorsGoldTransformer
from src.utils.aws_utils import S3Handler

# Desabilita logs durante testes
logging.basicConfig(level=logging.ERROR)

@pytest.mark.integration
def test_end_to_end_pipeline(setup_test_data):
    """
    Testa o pipeline completo: coleta → bronze → silver → gold.
    
    Este teste simula o fluxo completo do pipeline, usando dados mockados:
    1. Coleta de dados (mockada)
    2. Transformação Bronze para Silver
    3. Transformação Silver para Gold
    4. Validação dos resultados
    """
    # 1. Simula coleta de dados (mockada com setup_test_data)
    # O fixture setup_test_data já carregou os dados na camada bronze
    
    # 2. Transformação Bronze para Silver
    silver_transformer = EconomicIndicatorTransformer()
    silver_results = silver_transformer.process_all_indicators(['ipca', 'selic', 'pib', 'cambio'])
    
    # Verifica se as transformações para silver funcionaram
    assert 'ipca' in silver_results
    assert 'selic' in silver_results
    assert silver_results['ipca'] is True
    assert silver_results['selic'] is True
    
    # Valida existência de arquivos na camada silver
    s3_handler = S3Handler()
    silver_files = s3_handler.list_files(prefix='silver')
    assert len(silver_files) >= 2
    
    # 3. Transformação Silver para Gold
    gold_transformer = EconomicIndicatorsGoldTransformer()
    gold_result = gold_transformer.process_gold_layer()
    
    # Verifica se a transformação para gold funcionou
    assert gold_result is True
    
    # Valida existência de dashboards na camada gold
    gold_files = s3_handler.list_files(prefix='gold/dashboards')
    assert len(gold_files) > 0
    
    # 4. Validação dos resultados
    # Verifica o dashboard mensal
    monthly_files = [f for f in gold_files if 'monthly_indicators' in f]
    if monthly_files:
        df = s3_handler.download_file(monthly_files[0])
        assert df is not None
        assert 'year_month' in df.columns
        # Verifica se contém dados de múltiplos indicadores
        expected_columns = ['ipca', 'selic']
        present_columns = [col for col in expected_columns if col in df.columns]
        assert len(present_columns) > 0
    
    # Verifica o dashboard macroeconômico
    macro_files = [f for f in gold_files if 'macro_dashboard' in f]
    if macro_files:
        df = s3_handler.download_file(macro_files[0])
        assert df is not None
        assert 'indicator' in df.columns
        assert 'last_value' in df.columns
        
        # Verifica presença de indicadores
        indicators = df['indicator'].tolist()
        assert 'ipca' in indicators or 'selic' in indicators

@pytest.mark.integration
@patch.object(BCBCollector, 'get_series_data')
def test_collector_to_bronze(mock_get_series, bcb_collector, sample_ipca_data, setup_test_data):
    """
    Testa o fluxo do coletor até a camada bronze.
    
    Mockamos a coleta de dados para isolar esse teste dos serviços externos.
    """
    # Configura o mock para retornar dados conhecidos
    mock_get_series.return_value = sample_ipca_data
    
    # Executa a coleta
    results = bcb_collector.collect_and_store(indicators=['ipca'])
    
    # Verifica se a coleta foi bem-sucedida
    assert 'ipca' in results
    assert results['ipca'] is True
    
    # Verifica se os dados foram salvos na camada bronze
    s3_handler = S3Handler()
    bronze_files = s3_handler.list_files(prefix='bronze/bcb_indicators/ipca')
    
    # Deve haver pelo menos um arquivo (o que foi criado pelo setup_test_data)
    assert len(bronze_files) >= 1
    
    # Carrega os dados para verificar conteúdo
    df = s3_handler.download_file(bronze_files[0])
    assert df is not None
    assert 'ipca' in df.columns
    assert len(df) > 0

@pytest.mark.integration
def test_bronze_to_silver_to_gold(setup_test_data):
    """
    Testa fluxo Bronze → Silver → Gold para um único indicador.
    
    Este teste foca na transformação dos dados sem a coleta.
    """
    # 1. Transformação Bronze para Silver
    silver_transformer = EconomicIndicatorTransformer()
    silver_result = silver_transformer.process_indicator('ipca')
    
    # Verifica se a transformação foi bem-sucedida
    assert silver_result is True
    
    # Verifica existência de arquivo na camada silver
    s3_handler = S3Handler()
    silver_files = s3_handler.list_files(prefix='silver/economic_indicators/ipca')
    assert len(silver_files) > 0
    
    # Carrega arquivo para verificação
    silver_df = s3_handler.download_file(silver_files[0])
    assert silver_df is not None
    assert 'date' in silver_df.columns
    assert 'value' in silver_df.columns
    
    # 2. Transformação Silver para Gold
    gold_transformer = EconomicIndicatorsGoldTransformer()
    gold_result = gold_transformer.process_gold_layer()
    
    # Verifica se a transformação foi bem-sucedida
    assert gold_result is True
    
    # Verifica existência de dashboards na camada gold
    gold_files = s3_handler.list_files(prefix='gold/dashboards')
    assert len(gold_files) > 0