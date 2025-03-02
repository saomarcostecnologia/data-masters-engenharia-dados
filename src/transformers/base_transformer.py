# src/transformers/base_transformer.py
from abc import ABC, abstractmethod
import pandas as pd
import logging
from datetime import datetime
from typing import Dict, List, Optional, Any
import os

from ..utils.aws_utils import S3Handler
from ..utils.error_handling import error_handler, ProcessingError, ErrorCodes
from ..utils.helpers.logging_utils import get_logger, log_execution_time, log_dataframe_stats
from ..utils.helpers.data_validation import validate_dataset

class BaseTransformer(ABC):
    """
    Classe base para transformadores que fornece funcionalidades comuns.
    """
    
    def __init__(self, source_layer: str, target_layer: str, config: Dict[str, Any] = None):
        """
        Inicializa o transformador base.
        
        Args:
            source_layer: Camada fonte (bronze, silver)
            target_layer: Camada destino (silver, gold)
            config: Configurações adicionais (opcional)
        """
        self.source_layer = source_layer
        self.target_layer = target_layer
        self.config = config or {}
        
        # Inicializa o handler do S3
        self.s3_handler = S3Handler()
        
        # Configura logging
        self.logger = get_logger(f"{self.__class__.__name__}")
    
    @abstractmethod
    def transform(self, df: pd.DataFrame, indicator: str, **kwargs) -> pd.DataFrame:
        """
        Método abstrato que implementa a transformação específica para cada indicador.
        
        Args:
            df: DataFrame a ser transformado
            indicator: Nome do indicador
            **kwargs: Parâmetros adicionais
            
        Returns:
            DataFrame transformado
        """
        pass
    
    @error_handler(retries=2, retry_delay=5)
    def _load_source_data(self, prefix: str) -> Optional[pd.DataFrame]:
        """
        Carrega dados da camada fonte.
        
        Args:
            prefix: Prefixo do caminho no S3
            
        Returns:
            DataFrame com dados ou None se não encontrado
            
        Raises:
            ProcessingError: Em caso de erro ao carregar os dados
        """
        try:
            # Monta caminho completo
            full_prefix = f"{self.source_layer}/{prefix}"
            
            # Obtém arquivo mais recente
            latest_file = self.s3_handler.get_latest_file(full_prefix)
            
            if not latest_file:
                self.logger.warning(f"Nenhum arquivo encontrado em {full_prefix}")
                return None
                
            # Carrega o DataFrame
            self.logger.info(f"Carregando arquivo: {latest_file}")
            df = self.s3_handler.read_parquet(latest_file)
            
            if df is None or df.empty:
                raise ProcessingError(
                    message=f"Dados não disponíveis ou vazios em {latest_file}",
                    code=ErrorCodes.TRANSFORM_INPUT_ERROR
                )
                
            log_dataframe_stats(df, self.logger, f"DataFrame carregado de {latest_file}")
            return df
            
        except Exception as e:
            if not isinstance(e, ProcessingError):
                e = ProcessingError(
                    message=f"Erro ao carregar dados de {prefix}: {str(e)}",
                    code=ErrorCodes.S3_ACCESS_ERROR
                )
            self.logger.error(str(e))
            raise e
    
    @error_handler(retries=2, retry_delay=5)
    def _save_target_data(self, df: pd.DataFrame, prefix: str, 
                         partition_cols: List[str] = None) -> bool:
        """
        Salva dados na camada destino.
        
        Args:
            df: DataFrame a ser salvo
            prefix: Prefixo do caminho no S3
            partition_cols: Colunas para particionamento (opcional)
            
        Returns:
            bool: True se salvo com sucesso
            
        Raises:
            ProcessingError: Em caso de erro ao salvar os dados
        """
        try:
            # Verifica dataframe
            if df is None or df.empty:
                raise ProcessingError(
                    message="DataFrame vazio ou nulo",
                    code=ErrorCodes.TRANSFORM_OUTPUT_ERROR
                )
                
            # Monta caminho com timestamp
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            full_prefix = f"{self.target_layer}/{prefix}_{timestamp}"
            
            # Log
            self.logger.info(f"Salvando dados em {full_prefix}")
            
            # Se tem colunas de particionamento, usa método específico
            if partition_cols and len(partition_cols) > 0:
                # Implementar lógica de particionamento
                # Por enquanto usa método padrão
                pass
                
            # Faz upload
            success = self.s3_handler.write_parquet(df, full_prefix + ".parquet")
            
            if not success:
                raise ProcessingError(
                    message=f"Erro ao salvar dados em {full_prefix}",
                    code=ErrorCodes.TRANSFORM_OUTPUT_ERROR
                )
                
            self.logger.info(f"Dados salvos com sucesso em {full_prefix}")
            return True
            
        except Exception as e:
            if not isinstance(e, ProcessingError):
                e = ProcessingError(
                    message=f"Erro ao salvar dados em {prefix}: {str(e)}",
                    code=ErrorCodes.S3_ACCESS_ERROR
                )
            self.logger.error(str(e))
            raise e
    
    @log_execution_time(operation_name="Processamento de Indicador")
    def process_indicator(self, indicator: str, source_prefix: str = None, 
                        target_prefix: str = None, **kwargs) -> bool:
        """
        Processa um indicador da camada fonte para a camada destino.
        
        Args:
            indicator: Nome do indicador
            source_prefix: Prefixo personalizado para fonte (opcional)
            target_prefix: Prefixo personalizado para destino (opcional)
            **kwargs: Parâmetros adicionais para transformação
            
        Returns:
            bool: True se processado com sucesso
        """
        try:
            # Define prefixos
            src_prefix = source_prefix or f"economic_indicators/{indicator}"
            tgt_prefix = target_prefix or f"economic_indicators/{indicator}"
            
            # Carrega dados
            df = self._load_source_data(src_prefix)
            
            if df is None:
                self.logger.error(f"Não foi possível carregar dados para {indicator}")
                return False
                
            # Aplica transformações
            self.logger.info(f"Aplicando transformações para {indicator}")
            transformed_df = self.transform(df, indicator, **kwargs)
            
            if transformed_df is None or transformed_df.empty:
                self.logger.error(f"Transformação falhou para {indicator}")
                return False
                
            # Valida o DataFrame resultante
            validation_result = validate_dataset(
                transformed_df,
                required_columns=['date', 'value', 'indicator'],
                null_threshold_pct=10.0
            )
            
            if not validation_result[0]:
                self.logger.warning(f"Validação de dados encontrou problemas: {validation_result[1]}")
            
            # Salva resultado
            success = self._save_target_data(transformed_df, tgt_prefix)
            
            return success
            
        except Exception as e:
            self.logger.error(f"Erro ao processar indicador {indicator}: {str(e)}")
            return False
    
    def process_all_indicators(self, indicators: List[str] = None) -> Dict[str, bool]:
        """
        Processa múltiplos indicadores.
        
        Args:
            indicators: Lista de indicadores (se None, processa todos disponíveis)
            
        Returns:
            Dict: Status de cada indicador
        """
        if indicators is None:
            # Tenta descobrir indicadores disponíveis
            available_files = self.s3_handler.list_files(f"{self.source_layer}/economic_indicators")
            
            if not available_files:
                self.logger.warning("Nenhum arquivo encontrado para processar")
                return {}
                
            # Extrai nomes dos indicadores dos caminhos
            indicators = set()
            for file_path in available_files:
                parts = file_path.split('/')
                for part in parts:
                    if '_' in part:
                        indicator = part.split('_')[0]
                        indicators.add(indicator)
            
            indicators = list(indicators)
            
        self.logger.info(f"Processando indicadores: {indicators}")
        
        results = {}
        for indicator in indicators:
            success = self.process_indicator(indicator)
            results[indicator] = success
            
            status = "✅ Sucesso" if success else "❌ Falha"
            self.logger.info(f"Resultado para {indicator}: {status}")
            
        return results