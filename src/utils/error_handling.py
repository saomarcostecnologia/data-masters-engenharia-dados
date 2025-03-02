# src/utils/error_handling.py
"""Utilitários para tratamento padronizado de erros no projeto."""

import logging
import traceback
import functools
import time
from typing import Callable, Any, Optional, Dict, Type, List, Union

# Códigos de erro padronizados
class ErrorCodes:
    # Erros gerais
    UNKNOWN_ERROR = "ERR-GEN-001"
    VALIDATION_ERROR = "ERR-GEN-002"
    TIMEOUT_ERROR = "ERR-GEN-003"
    
    # Erros de AWS
    AWS_CONNECTION_ERROR = "ERR-AWS-001"
    S3_ACCESS_ERROR = "ERR-AWS-002"
    S3_NOT_FOUND_ERROR = "ERR-AWS-003"
    
    # Erros de coleta
    COLLECTOR_CONNECTION_ERROR = "ERR-COL-001"
    COLLECTOR_API_ERROR = "ERR-COL-002"
    COLLECTOR_DATA_FORMAT_ERROR = "ERR-COL-003"
    
    # Erros de transformação
    TRANSFORM_INPUT_ERROR = "ERR-TRF-001"
    TRANSFORM_PROCESSING_ERROR = "ERR-TRF-002"
    TRANSFORM_OUTPUT_ERROR = "ERR-TRF-003"

class ProcessingError(Exception):
    """Exceção personalizada para erros no processamento de dados."""
    
    def __init__(self, message: str, code: str = ErrorCodes.UNKNOWN_ERROR, 
                 details: Dict[str, Any] = None):
        """
        Inicializa uma exceção de processamento.
        
        Args:
            message: Mensagem de erro
            code: Código de erro padronizado
            details: Detalhes adicionais do erro
        """
        self.code = code
        self.details = details or {}
        super().__init__(message)

def error_handler(logger: logging.Logger = None, 
                  retries: int = 0, 
                  retry_delay: int = 5,
                  handled_exceptions: List[Type[Exception]] = None):
    """
    Decorator para manipulação padronizada de erros.
    
    Args:
        logger: Logger a ser usado (se None, cria um novo)
        retries: Número de retentativas em caso de erro
        retry_delay: Tempo de espera entre retentativas (segundos)
        handled_exceptions: Lista de exceções que devem ser capturadas
        
    Returns:
        Decorator
    """
    if logger is None:
        logger = logging.getLogger(__name__)
        
    if handled_exceptions is None:
        handled_exceptions = [Exception]
    
    def decorator(func: Callable):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            retry_count = 0
            
            while True:
                try:
                    return func(*args, **kwargs)
                except tuple(handled_exceptions) as e:
                    retry_count += 1
                    
                    # Log do erro com detalhes
                    error_message = f"Erro em {func.__name__}: {str(e)}"
                    stack_trace = traceback.format_exc()
                    
                    # Se excedeu número de retentativas ou não deve tentar novamente
                    if retry_count > retries:
                        logger.error(f"{error_message}\n{stack_trace}")
                        
                        # Reempacota o erro para um formato padronizado se não for um ProcessingError
                        if not isinstance(e, ProcessingError):
                            if isinstance(e, ConnectionError):
                                code = ErrorCodes.COLLECTOR_CONNECTION_ERROR
                            elif "AWS" in str(e) or "S3" in str(e):
                                code = ErrorCodes.S3_ACCESS_ERROR
                            else:
                                code = ErrorCodes.UNKNOWN_ERROR
                                
                            raise ProcessingError(
                                message=f"Erro em {func.__name__}: {str(e)}",
                                code=code,
                                details={"original_error": str(e), "traceback": stack_trace}
                            )
                        raise
                    
                    # Log de tentativa de nova tentativa
                    logger.warning(
                        f"{error_message}. Tentativa {retry_count}/{retries}. "
                        f"Aguardando {retry_delay}s para retry."
                    )
                    time.sleep(retry_delay)
                    
        return wrapper
    
    return decorator