# src/collectors/base_collector.py
from abc import abstractmethod
import pandas as pd
import logging
from datetime import datetime
from typing import Dict, List, Optional, Union, Any
import os

from .abstract_collector import AbstractCollector
from ..utils.aws_utils import S3Handler
from ..utils.error_handling import error_handler, ProcessingError, ErrorCodes
from ..utils.helpers.logging_utils import get_logger

class BaseCollector(AbstractCollector):
    """
    Implementação base para coletores que fornece funcionalidades comuns.
    Segue o padrão Template Method, deixando métodos específicos para serem implementados
    por classes concretas.
    """
    
    def __init__(self, config: Dict[str, Any] = None):
        """
        Inicializa o coletor base com funcionalidades comuns.
        
        Args:
            config: Configurações adicionais (opcional)
        """
        # Inicializa o handler do S3
        self.s3_handler = S3Handler()
        self.config = config or {}
        
        # Configura logging
        self.logger = get_logger(f"{self.__class__.__name__}")
    
    @error_handler(retries=3, retry_delay=5)
    def _store_data(self, df: pd.DataFrame, source: str, indicator: str) -> bool:
        """
        Armazena os dados coletados no S3 com tratamento de erros aprimorado.
        
        Args:
            df: DataFrame com dados coletados
            source: Nome da fonte de dados
            indicator: Nome do indicador
            
        Returns:
            bool: True se o armazenamento foi bem sucedido
        
        Raises:
            ProcessingError: Erro padronizado durante o armazenamento
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
            
            if not success:
                raise ProcessingError(
                    message=f"Falha no armazenamento dos dados para {indicator}",
                    code=ErrorCodes.S3_ACCESS_ERROR
                )
                
            return success
            
        except Exception as e:
            # Captura exceções não tratadas e as converte para ProcessingError
            if not isinstance(e, ProcessingError):
                e = ProcessingError(
                    message=f"Erro ao armazenar dados: {str(e)}",
                    code=ErrorCodes.S3_ACCESS_ERROR,
                    details={"indicator": indicator, "source": source}
                )
            self._log_error(str(e))
            raise e
    
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
    
    def _log_warning(self, message: str) -> None:
        """
        Registra mensagem de aviso no log.
        
        Args:
            message: Mensagem de aviso a ser registrada
        """
        self.logger.warning(message)
        
    def _validate_response(self, response, indicator: str) -> bool:
        """
        Valida a resposta de uma API para garantir qualidade dos dados.
        
        Args:
            response: Resposta da API
            indicator: Nome do indicador
            
        Returns:
            bool: True se a resposta é válida
            
        Raises:
            ProcessingError: Se a resposta for inválida
        """
        if response is None:
            raise ProcessingError(
                message=f"Resposta nula para {indicator}",
                code=ErrorCodes.COLLECTOR_API_ERROR
            )
            
        if hasattr(response, 'status_code') and response.status_code != 200:
            raise ProcessingError(
                message=f"Status code inválido ({response.status_code}) para {indicator}",
                code=ErrorCodes.COLLECTOR_API_ERROR,
                details={"status_code": response.status_code}
            )
            
        return True
    
    def _validate_dataframe(self, df: pd.DataFrame, indicator: str) -> bool:
        """
        Valida se o DataFrame é válido e contém os dados necessários.
        
        Args:
            df: DataFrame a ser validado
            indicator: Nome do indicador
            
        Returns:
            bool: True se o DataFrame é válido
            
        Raises:
            ProcessingError: Se o DataFrame for inválido
        """
        if df is None or df.empty:
            raise ProcessingError(
                message=f"DataFrame vazio para {indicator}",
                code=ErrorCodes.COLLECTOR_DATA_FORMAT_ERROR
            )
            
        return True
    
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