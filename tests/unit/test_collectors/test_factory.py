"""Testes unitários para o CollectorFactory."""

import pytest
from unittest.mock import patch, MagicMock

from src.collectors.factory import CollectorFactory
from src.collectors.bcb_collector import BCBCollector
from src.collectors.ibge_collector import IBGECollector
from src.collectors.abstract_collector import AbstractCollector

def test_get_collector(collector_factory):
    """Testa obtenção de coletores existentes."""
    # Testa obtenção do coletor BCB
    bcb_collector = collector_factory.get_collector('bcb')
    assert bcb_collector is not None
    assert isinstance(bcb_collector, BCBCollector)
    
    # Testa obtenção do coletor IBGE
    ibge_collector = collector_factory.get_collector('ibge')
    assert ibge_collector is not None
    assert isinstance(ibge_collector, IBGECollector)
    
    # Testa obtenção de coletor inexistente
    invalid_collector = collector_factory.get_collector('invalid')
    assert invalid_collector is None

def test_list_collectors(collector_factory):
    """Testa listagem de coletores disponíveis."""
    collectors = collector_factory.list_collectors()
    assert 'bcb' in collectors
    assert 'ibge' in collectors
    assert len(collectors) >= 2

def test_get_all_collectors(collector_factory):
    """Testa obtenção de todos os coletores."""
    collectors = collector_factory.get_all_collectors()
    assert 'bcb' in collectors
    assert 'ibge' in collectors
    assert isinstance(collectors['bcb'], BCBCollector)
    assert isinstance(collectors['ibge'], IBGECollector)

def test_register_collector(collector_factory):
    """Testa registro de novo coletor."""
    # Cria uma classe de coletor mock
    class MockCollector(AbstractCollector):
        def get_source_name(self): return 'mock'
        def get_available_indicators(self): return {}
        def get_series_data(self, *args, **kwargs): return None
        def _store_data(self, *args, **kwargs): return True
        def _log_info(self, *args, **kwargs): pass
        def _log_error(self, *args, **kwargs): pass
    
    # Registra o novo coletor
    collector_factory.register_collector('mock', MockCollector)
    
    # Verifica se o coletor foi registrado
    assert 'mock' in collector_factory.list_collectors()
    
    # Obtém e verifica o novo coletor
    mock_collector = collector_factory.get_collector('mock')
    assert mock_collector is not None
    assert isinstance(mock_collector, MockCollector)
    
    # Limpa o registro para não afetar outros testes
    collector_factory._collectors.pop('mock', None)