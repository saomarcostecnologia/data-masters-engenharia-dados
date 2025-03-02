# src/collectors/base_collector.py
import pandas as pd
import logging
from datetime import datetime
from typing import Dict, List, Optional, Union, Any
import os
from abc import abstractmethod

from .abstract_collector import AbstractCollector
from ..utils.aws_utils import S3Handler

class BaseCollector(AbstractCollector):
    """
    Implementação base para coletores que fornece funcionalidades comuns.
    Segue o padrão Template Method, deixando métodos específicos para serem implementados
    por classes concretas.
    """
    
    def __init__(self):
        """Inicializa o coletor base com funcionalidades comuns."""
        # Inicializa o handler do S3
        self.s3_handler = S3Handler()
        
        # Configura logging
        self.logger = logging.getLogger(f"{self.__class__.__name__}")
        self._setup_logging()
    
    def _setup_logging(self):
        """Configura o logger para o coletor."""
        # Verifica se já tem handlers configurados
        if not self.logger.handlers:
            # Configura logging para stdout
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            
            self.logger.addHandler(handler)
            self.logger.setLevel(logging.INFO)
    
    def _store_data(self, df: pd.DataFrame, source: str, indicator: str) -> bool:
        """
        Armazena os dados coletados no S3.
        
        Args:
            df: DataFrame com dados coletados
            source: Nome da fonte de dados
            indicator: Nome do indicador
            
        Returns:
            bool: True se o armazenamento foi bem sucedido
        """
        try:
            # Define o caminho no S3
            file_path = f"{source}_indicators/{indicator}"
            
            # Upload para S3 na camada Bronze
            success = self.s3_handler.upload_dataframe(
                df=df,
                file_path=file_path,
                layer='bronze',
                format='parquet'
            )
            
            return success
            
        except Exception as e:
            self._log_error(f"Erro ao armazenar dados: {str(e)}")
            return False
    
    def _log_info(self, message: str) -> None:
        """
        Registra mensagem informativa no log.
        
        Args:
            message: Mensagem a ser registrada
        """
        self.logger.info(message)
    
    def _log_error(self, message: str) -> None:
        """
        Registra mensagem de erro no log.
        
        Args:
            message: Mensagem de erro a ser registrada
        """
        self.logger.error(message)
    
    @abstractmethod
    def get_source_name(self) -> str:
        """
        Retorna o nome da fonte de dados.
        Deve ser implementado por classes concretas.
        
        Returns:
            str: Nome da fonte de dados
        """
        pass
    
    @abstractmethod
    def get_available_indicators(self) -> Dict[str, Dict[str, Any]]:
        """
        Retorna os indicadores disponíveis para esta fonte.
        Deve ser implementado por classes concretas.
        
        Returns:
            Dict: Mapeamento de indicadores para configurações
        """
        pass
    
    @abstractmethod
    def get_series_data(self, 
                      indicator: str,
                      start_date: Optional[datetime] = None,
                      end_date: Optional[datetime] = None,
                      **kwargs) -> Optional[pd.DataFrame]:
        """
        Coleta dados de uma série específica.
        Deve ser implementado por classes concretas.
        
        Args:
            indicator: Nome do indicador
            start_date: Data inicial
            end_date: Data final
            
        Returns:
            DataFrame com dados ou None em caso de erro
        """
        pass