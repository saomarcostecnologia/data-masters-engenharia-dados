# src/utils/helpers/logging_utils.py
import logging
import sys
import os
from datetime import datetime
from typing import Dict, Optional, Any, Union

def setup_logging(
    log_level: str = 'INFO',
    log_format: str = None,
    log_file: str = None,
    log_to_console: bool = True,
    app_name: str = 'economic-indicators'
) -> logging.Logger:
    """
    Configura logging com opções personalizadas.
    
    Args:
        log_level: Nível de logging (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_format: Formato personalizado para logs
        log_file: Caminho para arquivo de log
        log_to_console: Se deve logar também no console
        app_name: Nome da aplicação para o logger
        
    Returns:
        Logger configurado
    """
    # Mapeia string de nível para constante do logging
    level_map = {
        'DEBUG': logging.DEBUG,
        'INFO': logging.INFO,
        'WARNING': logging.WARNING,
        'ERROR': logging.ERROR,
        'CRITICAL': logging.CRITICAL
    }
    
    log_level_value = level_map.get(log_level.upper(), logging.INFO)
    
    # Define formato padrão se não especificado
    if log_format is None:
        log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        
    # Cria formatter
    formatter = logging.Formatter(log_format)
    
    # Configura logger
    logger = logging.getLogger(app_name)
    logger.setLevel(log_level_value)
    
    # Remove handlers existentes para evitar duplicação
    if logger.hasHandlers():
        logger.handlers.clear()
    
    # Adiciona handler de arquivo se especificado
    if log_file:
        # Cria diretório para o arquivo de log se não existir
        log_dir = os.path.dirname(log_file)
        if log_dir and not os.path.exists(log_dir):
            os.makedirs(log_dir)
            
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    # Adiciona handler de console se solicitado
    if log_to_console:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
        
    return logger

def get_logger(name: str = None) -> logging.Logger:
    """
    Obtém um logger configurado.
    
    Args:
        name: Nome do logger
        
    Returns:
        Logger configurado
    """
    # Define nome padrão se não especificado
    if name is None:
        name = 'economic-indicators'
    
    logger = logging.getLogger(name)
    
    # Se o logger não tiver handlers, configura com padrões
    if not logger.hasHandlers():
        setup_logging(app_name=name)
        
    return logger

def log_execution_time(
    logger: logging.Logger = None,
    operation_name: str = "Operação"
) -> callable:
    """
    Decorator para registrar tempo de execução de funções.
    
    Args:
        logger: Logger a ser usado (se None, cria um)
        operation_name: Nome da operação para o log
        
    Returns:
        Decorator
    """
    if logger is None:
        logger = get_logger()
    
    def decorator(func):
        def wrapper(*args, **kwargs):
            # Registra início
            start_time = datetime.now()
            logger.info(f"{operation_name} iniciada")
            
            try:
                # Executa função
                result = func(*args, **kwargs)
                
                # Registra conclusão com sucesso
                end_time = datetime.now()
                duration = (end_time - start_time).total_seconds()
                logger.info(f"{operation_name} concluída com sucesso em {duration:.2f} segundos")
                
                return result
                
            except Exception as e:
                # Registra erro
                end_time = datetime.now()
                duration = (end_time - start_time).total_seconds()
                logger.error(f"{operation_name} falhou após {duration:.2f} segundos. Erro: {str(e)}")
                raise
                
        return wrapper
    
    return decorator

def log_dataframe_stats(
    df,
    logger: logging.Logger = None,
    label: str = "DataFrame"
) -> None:
    """
    Loga estatísticas básicas de um DataFrame.
    
    Args:
        df: DataFrame a ser analisado
        logger: Logger a ser usado (se None, cria um)
        label: Rótulo para identificar o DataFrame
    """
    if logger is None:
        logger = get_logger()
        
    if df is None:
        logger.warning(f"{label} é None")
        return
        
    if df.empty:
        logger.warning(f"{label} está vazio")
        return
        
    # Loga estatísticas básicas
    logger.info(f"=== Estatísticas: {label} ===")
    logger.info(f"Dimensões: {df.shape}")
    logger.info(f"Colunas: {df.columns.tolist()}")
    logger.info(f"Tipos de dados: {df.dtypes.to_dict()}")
    logger.info(f"Valores nulos (total): {df.isnull().sum().sum()}")
    
    # Estatística de valores nulos por coluna se houver
    if df.isnull().sum().sum() > 0:
        null_counts = df.isnull().sum()
        null_cols = null_counts[null_counts > 0].to_dict()
        logger.info(f"Valores nulos por coluna: {null_cols}")
        
    # Estatísticas para colunas numéricas
    num_cols = df.select_dtypes(include=['number']).columns
    if len(num_cols) > 0:
        logger.info(f"Estatísticas numéricas: {df[num_cols].describe().to_dict()}")

def log_process_result(
    logger: logging.Logger,
    process_name: str,
    success: bool,
    details: Dict[str, Any] = None
) -> None:
    """
    Loga resultado de um processo.
    
    Args:
        logger: Logger a ser usado
        process_name: Nome do processo
        success: Se o processo foi bem-sucedido
        details: Detalhes adicionais do processo
    """
    if details is None:
        details = {}
        
    if success:
        logger.info(f"✅ {process_name} concluído com sucesso.")
        
        if details:
            for key, value in details.items():
                logger.info(f"  - {key}: {value}")
    else:
        logger.error(f"❌ {process_name} falhou.")
        
        if details:
            for key, value in details.items():
                logger.error(f"  - {key}: {value}")