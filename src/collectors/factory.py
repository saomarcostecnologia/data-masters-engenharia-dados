# src/collectors/factory.py
from typing import Dict, Optional, List, Any
import logging

from .abstract_collector import AbstractCollector
from .bcb_collector import BCBCollector
from .ibge_collector import IBGECollector

class CollectorFactory:
    """
    Factory responsável por criar instâncias de coletores.
    Implementa o padrão Factory Method para criar diferentes tipos de coletores.
    """
    
    # Registro de tipos de coletores disponíveis
    _collectors = {
        'bcb': BCBCollector,
        'ibge': IBGECollector
    }
    
    @classmethod
    def register_collector(cls, source_name: str, collector_class: Any) -> None:
        """
        Registra um novo tipo de coletor no factory.
        
        Args:
            source_name: Nome da fonte de dados
            collector_class: Classe do coletor a ser registrado
        """
        cls._collectors[source_name] = collector_class
        logging.info(f"Coletor {source_name} registrado com sucesso")
    
    @classmethod
    def get_collector(cls, source_name: str) -> Optional[AbstractCollector]:
        """
        Cria e retorna uma instância do coletor solicitado.
        
        Args:
            source_name: Nome da fonte de dados
            
        Returns:
            Instância do coletor ou None se não encontrado
        """
        if source_name not in cls._collectors:
            logging.error(f"Coletor para fonte {source_name} não registrado")
            return None
            
        # Cria instância do coletor
        return cls._collectors[source_name]()
    
    @classmethod
    def list_collectors(cls) -> List[str]:
        """
        Lista todos os coletores disponíveis.
        
        Returns:
            Lista com nomes das fontes de dados disponíveis
        """
        return list(cls._collectors.keys())
    
    @classmethod
    def get_all_collectors(cls) -> Dict[str, AbstractCollector]:
        """
        Cria e retorna instâncias de todos os coletores disponíveis.
        
        Returns:
            Dicionário com instâncias de todos os coletores
        """
        collectors = {}
        for source_name in cls._collectors:
            collectors[source_name] = cls.get_collector(source_name)
            
        return collectors