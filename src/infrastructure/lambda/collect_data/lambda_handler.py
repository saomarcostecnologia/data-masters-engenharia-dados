# infrastructure/lambda/collect_data/lambda_handler.py
import os
import sys
import json
import logging
import boto3
import importlib
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional

# Configura logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Importação dinâmica para permitir que o código do projeto seja usado
def init_project_imports():
    """
    Adiciona o diretório do projeto ao sys.path para permitir 
    importações dos módulos do projeto.
    """
    # Se o código do projeto está no mesmo pacote Lambda (layer)
    if os.path.exists('/opt/python'):
        sys.path.append('/opt/python')
    
    # Adiciona o diretório atual
    current_dir = os.path.dirname(os.path.abspath(__file__))
    if current_dir not in sys.path:
        sys.path.append(current_dir)

    # Para ambiente de desenvolvimento local
    if os.environ.get('AWS_EXECUTION_ENV') is None:
        # Assumindo que o código está no diretório do repositório
        project_root = os.path.abspath(os.path.join(current_dir, '..', '..', '..'))
        if project_root not in sys.path:
            sys.path.append(project_root)

def get_collector(source: str):
    """
    Importa dinamicamente o Factory e cria uma instância do coletor.
    
    Args:
        source: Nome da fonte de dados
        
    Returns:
        Instância do coletor ou None se não encontrado
    """
    try:
        # Importa o Factory de coletores
        CollectorFactory = importlib.import_module('src.collectors.factory').CollectorFactory
        
        # Obtém o coletor para a fonte
        return CollectorFactory.get_collector(source)
    except Exception as e:
        logger.error(f"Erro ao importar ou criar coletor para {source}: {str(e)}")
        return None

def handler(event, context):
    """
    Handler da função Lambda para coleta de dados.
    
    Args:
        event: Evento de acionamento da Lambda
        context: Contexto de execução
        
    Returns:
        Dict com status e detalhes da execução
    """
    # Inicializa importações
    init_project_imports()
    
    try:
        # Log do evento recebido
        logger.info(f"Evento recebido: {json.dumps(event)}")
        
        # Obtém parâmetros
        source = event.get('source')
        if not source:
            return {
                'success': False,
                'error': 'Parâmetro source não especificado'
            }
            
        # Obtém outros parâmetros opcionais
        indicators = event.get('indicators', 'all')
        months = int(event.get('months', 12))
        environment = event.get('environment', 'dev')
        
        # Configura ambiente
        os.environ['ENVIRONMENT'] = environment
        
        # Obtém configuração do S3
        s3_bucket = event.get('s3Bucket', os.environ.get('DATA_LAKE_BUCKET'))
        if s3_bucket:
            os.environ['AWS_BUCKET_NAME'] = s3_bucket
        
        # Calcula o intervalo de datas
        end_date = datetime.now()
        start_date = end_date - timedelta(days=30.5 * months)
        
        # Prepara lista de indicadores
        indicator_list = indicators.split(',') if isinstance(indicators, str) and indicators != 'all' else None
        
        # Obtém o coletor
        collector = get_collector(source)
        if not collector:
            return {
                'success': False,
                'error': f'Coletor para fonte {source} não encontrado'
            }
        
        # Executa coleta de dados
        results = collector.collect_and_store(
            indicators=indicator_list,
            start_date=start_date,
            end_date=end_date
        )
        
        # Calcula status geral
        success = any(status for status in results.values()) if results else False
        
        # Prepara resposta
        response = {
            'success': success,
            'source': source,
            'indicators_collected': results,
            'start_date': start_date.isoformat(),
            'end_date': end_date.isoformat(),
            'environment': environment
        }
        
        return response
        
    except Exception as e:
        logger.error(f"Erro durante execução: {str(e)}")
        import traceback
        traceback.print_exc()
        
        return {
            'success': False,
            'error': str(e)
        }