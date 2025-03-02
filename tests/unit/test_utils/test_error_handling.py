"""Testes unitários para o sistema de tratamento de erros."""

import pytest
import logging
import time
from unittest.mock import patch, MagicMock

from src.utils.error_handling import (
    ProcessingError, 
    ErrorCodes, 
    error_handler
)

def test_processing_error_init():
    """Testa a inicialização da classe ProcessingError."""
    # Cria uma exceção básica
    error = ProcessingError("Erro de teste")
    assert str(error) == "Erro de teste"
    assert error.code == ErrorCodes.UNKNOWN_ERROR
    assert error.details == {}
    
    # Cria uma exceção com código e detalhes
    error = ProcessingError(
        message="Erro de conexão",
        code=ErrorCodes.AWS_CONNECTION_ERROR,
        details={"service": "S3", "attempt": 3}
    )
    assert str(error) == "Erro de conexão"
    assert error.code == ErrorCodes.AWS_CONNECTION_ERROR
    assert error.details["service"] == "S3"
    assert error.details["attempt"] == 3

def test_error_handler_success():
    """Testa o decorator error_handler em caso de sucesso."""
    # Cria um logger de teste
    logger = logging.getLogger("test_logger")
    
    # Define uma função bem-sucedida
    @error_handler(logger=logger)
    def successful_function():
        return "Sucesso"
    
    # Executa a função
    result = successful_function()
    
    # Verifica o resultado
    assert result == "Sucesso"

def test_error_handler_exception():
    """Testa o decorator error_handler em caso de exceção."""
    # Cria um logger de teste
    logger = logging.getLogger("test_logger")
    
    # Define uma função que lança exceção
    @error_handler(logger=logger)
    def failing_function():
        raise ValueError("Erro de teste")
    
    # Verifica se a exceção é propagada corretamente
    with pytest.raises(ProcessingError) as excinfo:
        failing_function()
    
    # Verifica se a exceção foi convertida para ProcessingError
    assert isinstance(excinfo.value, ProcessingError)
    assert "Erro de teste" in str(excinfo.value)

def test_error_handler_retry():
    """Testa a funcionalidade de retry do decorator error_handler."""
    # Cria um logger de teste
    logger = logging.getLogger("test_logger")
    
    # Cria um mock para rastrear chamadas
    mock_function = MagicMock()
    mock_function.side_effect = [
        ValueError("Falha na primeira tentativa"),
        ValueError("Falha na segunda tentativa"),
        "Sucesso na terceira tentativa"
    ]
    
    # Define uma função com retry
    @error_handler(logger=logger, retries=2, retry_delay=0.1)
    def retrying_function():
        return mock_function()
    
    # Executa a função
    result = retrying_function()
    
    # Verifica o resultado
    assert result == "Sucesso na terceira tentativa"
    assert mock_function.call_count == 3

def test_error_handler_retry_exceeded():
    """Testa quando o número máximo de retentativas é excedido."""
    # Cria um logger de teste
    logger = logging.getLogger("test_logger")
    
    # Cria um mock que sempre falha
    mock_function = MagicMock()
    mock_function.side_effect = ValueError("Sempre falha")
    
    # Define uma função com retry
    @error_handler(logger=logger, retries=2, retry_delay=0.1)
    def always_failing_function():
        return mock_function()
    
    # Verifica se a exceção é propagada após todas as tentativas
    with pytest.raises(ProcessingError):
        always_failing_function()
    
    # Verifica se o número correto de tentativas foi feito
    assert mock_function.call_count == 3  # 1 original + 2 retentativas

def test_error_handler_processing_error():
    """Testa que ProcessingError não é reempacotada."""
    # Cria um logger de teste
    logger = logging.getLogger("test_logger")
    
    # Define uma função que lança ProcessingError
    @error_handler(logger=logger)
    def function_with_processing_error():
        raise ProcessingError(
            message="Erro específico",
            code=ErrorCodes.COLLECTOR_API_ERROR,
            details={"api": "test_api"}
        )
    
    # Verifica se a exceção é propagada sem modificação
    with pytest.raises(ProcessingError) as excinfo:
        function_with_processing_error()
    
    # Verifica se os detalhes da exceção são mantidos
    assert excinfo.value.code == ErrorCodes.COLLECTOR_API_ERROR
    assert excinfo.value.details["api"] == "test_api"

def test_error_handler_custom_exceptions():
    """Testa o tratamento de exceções específicas."""
    # Cria um logger de teste
    logger = logging.getLogger("test_logger")
    
    # Define uma função que lança diferentes tipos de exceções
    @error_handler(logger=logger, handled_exceptions=[ValueError, KeyError])
    def function_with_specific_exceptions(exception_type):
        if exception_type == "value":
            raise ValueError("Erro de valor")
        elif exception_type == "key":
            raise KeyError("Erro de chave")
        elif exception_type == "type":
            raise TypeError("Erro de tipo")
        else:
            return "Sucesso"
    
    # Verifica tratamento de ValueError
    with pytest.raises(ProcessingError):
        function_with_specific_exceptions("value")
    
    # Verifica tratamento de KeyError
    with pytest.raises(ProcessingError):
        function_with_specific_exceptions("key")
    
    # Verifica que TypeError não é tratada
    with pytest.raises(TypeError):
        function_with_specific_exceptions("type")
    
    # Verifica caso de sucesso
    assert function_with_specific_exceptions("none") == "Sucesso"