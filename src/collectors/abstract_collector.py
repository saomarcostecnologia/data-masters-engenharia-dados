# src/collectors/abstract_collector.py
from abc import ABC, abstractmethod
import pandas as pd
from datetime import datetime
from typing import Dict, List, Optional, Union, Any

class AbstractCollector(ABC):
    """
    Interface abstrata que define o contrato para todos os coletores de dados.
    Implementa o padrão Template Method para padronizar o processo de coleta.
    """
    
    @abstractmethod
    def get_source_name(self) -> str:
        """
        Retorna o nome da fonte de dados (ex: 'bcb', 'ibge').
        
        Returns:
            str: Nome da fonte de dados
        """
        pass
        
    @abstractmethod
    def get_available_indicators(self) -> Dict[str, Dict[str, Any]]:
        """
        Retorna um dicionário com os indicadores disponíveis e suas configurações.
        
        Returns:
            Dict[str, Dict[str, Any]]: Mapeamento de indicadores para suas configurações
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
        
        Args:
            indicator: Nome do indicador econômico
            start_date: Data inicial para coleta
            end_date: Data final para coleta
            **kwargs: Parâmetros adicionais específicos da fonte
            
        Returns:
            DataFrame com os dados coletados ou None em caso de erro
        """
        pass
        
    def collect_and_store(self, 
                        indicators: Optional[List[str]] = None,
                        start_date: Optional[datetime] = None,
                        end_date: Optional[datetime] = None,
                        **kwargs) -> Dict[str, bool]:
        """
        Coleta e armazena dados de todos os indicadores especificados.
        Implementa o padrão Template Method para definir o fluxo geral de coleta.
        
        Args:
            indicators: Lista de indicadores para coletar (se None, coleta todos disponíveis)
            start_date: Data inicial para coleta
            end_date: Data final para coleta
            **kwargs: Parâmetros adicionais específicos da fonte
            
        Returns:
            Dicionário com status de sucesso para cada indicador
        """
        # Se não especificado, coleta todos os indicadores disponíveis
        if indicators is None:
            indicators = list(self.get_available_indicators().keys())
            
        results = {}
        source_name = self.get_source_name()
        
        for indicator in indicators:
            try:
                # Hook de pré-processamento (pode ser sobrescrito)
                self._pre_collect_hook(indicator, start_date, end_date, **kwargs)
                
                # Coleta os dados
                df = self.get_series_data(indicator, start_date, end_date, **kwargs)
                
                if df is None or df.empty:
                    self._log_error(f"Dados não disponíveis para {indicator}")
                    results[indicator] = False
                    continue
                
                # Hook de pós-processamento (pode ser sobrescrito)
                df = self._post_collect_hook(df, indicator, **kwargs)
                
                # Armazena os dados
                success = self._store_data(df, source_name, indicator)
                results[indicator] = success
                
                if success:
                    self._log_info(f"Dados de {indicator} processados com sucesso")
                else:
                    self._log_error(f"Falha no armazenamento dos dados de {indicator}")
                    
            except Exception as e:
                self._log_error(f"Erro ao processar {indicator}: {str(e)}")
                results[indicator] = False
                
        return results
    
    def _pre_collect_hook(self, indicator: str, start_date: Optional[datetime], 
                         end_date: Optional[datetime], **kwargs) -> None:
        """
        Hook executado antes da coleta de dados.
        Pode ser sobrescrito por implementações específicas.
        
        Args:
            indicator: Nome do indicador
            start_date: Data inicial
            end_date: Data final
            **kwargs: Parâmetros adicionais
        """
        pass
    
    def _post_collect_hook(self, df: pd.DataFrame, indicator: str, **kwargs) -> pd.DataFrame:
        """
        Hook executado após a coleta de dados.
        Pode ser sobrescrito para realizar transformações específicas.
        
        Args:
            df: DataFrame com dados coletados
            indicator: Nome do indicador
            **kwargs: Parâmetros adicionais
            
        Returns:
            DataFrame processado
        """
        return df
    
    @abstractmethod
    def _store_data(self, df: pd.DataFrame, source: str, indicator: str) -> bool:
        """
        Armazena os dados coletados (por exemplo, no S3).
        
        Args:
            df: DataFrame com dados coletados
            source: Nome da fonte de dados
            indicator: Nome do indicador
            
        Returns:
            bool: True se o armazenamento foi bem sucedido
        """
        pass
    
    @abstractmethod
    def _log_info(self, message: str) -> None:
        """
        Registra mensagem informativa no log.
        
        Args:
            message: Mensagem a ser registrada
        """
        pass
    
    @abstractmethod
    def _log_error(self, message: str) -> None:
        """
        Registra mensagem de erro no log.
        
        Args:
            message: Mensagem de erro a ser registrada
        """
        pass