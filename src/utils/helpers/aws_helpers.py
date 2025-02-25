# src/utils/helpers/aws_helpers.py
import boto3
import logging
import pandas as pd
import io
from typing import Dict, List, Optional, Union, Tuple, Any
from datetime import datetime
import os
from botocore.exceptions import ClientError

def get_s3_client():
    """
    Obtém um cliente S3 autenticado.
    
    Returns:
        boto3.client: Cliente S3 configurado
    """
    try:
        return boto3.client(
            's3',
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
            region_name=os.getenv('AWS_REGION')
        )
    except Exception as e:
        logging.error(f"Erro ao criar cliente S3: {str(e)}")
        raise

def list_s3_files(bucket: str, prefix: str = '') -> List[str]:
    """
    Lista arquivos em um bucket S3.
    
    Args:
        bucket: Nome do bucket
        prefix: Prefixo para filtrar arquivos
        
    Returns:
        Lista de chaves (paths) dos objetos
    """
    try:
        s3_client = get_s3_client()
        
        response = s3_client.list_objects_v2(
            Bucket=bucket,
            Prefix=prefix
        )
        
        files = []
        if 'Contents' in response:
            files = [obj['Key'] for obj in response['Contents']]
                
        return files
        
    except Exception as e:
        logging.error(f"Erro ao listar arquivos em {bucket}/{prefix}: {str(e)}")
        return []

def get_latest_s3_file(bucket: str, prefix: str) -> Optional[str]:
    """
    Obtém o arquivo mais recente em um caminho do S3.
    
    Args:
        bucket: Nome do bucket
        prefix: Prefixo/caminho dos arquivos
        
    Returns:
        Path do arquivo mais recente ou None
    """
    try:
        files = list_s3_files(bucket, prefix)
        
        if not files:
            logging.warning(f"Nenhum arquivo encontrado em {bucket}/{prefix}")
            return None
            
        # Ordena para pegar o mais recente (presumindo nomeação cronológica)
        return sorted(files)[-1]
        
    except Exception as e:
        logging.error(f"Erro ao obter arquivo mais recente: {str(e)}")
        return None

def read_parquet_from_s3(bucket: str, key: str) -> Optional[pd.DataFrame]:
    """
    Lê arquivo Parquet do S3 como DataFrame.
    
    Args:
        bucket: Nome do bucket
        key: Caminho/chave do arquivo
        
    Returns:
        DataFrame ou None se ocorrer erro
    """
    try:
        s3_client = get_s3_client()
        
        response = s3_client.get_object(
            Bucket=bucket,
            Key=key
        )
        
        buffer = io.BytesIO(response['Body'].read())
        return pd.read_parquet(buffer)
        
    except Exception as e:
        logging.error(f"Erro ao ler parquet {bucket}/{key}: {str(e)}")
        return None

def read_csv_from_s3(bucket: str, key: str, **kwargs) -> Optional[pd.DataFrame]:
    """
    Lê arquivo CSV do S3 como DataFrame.
    
    Args:
        bucket: Nome do bucket
        key: Caminho/chave do arquivo
        **kwargs: Argumentos adicionais para pd.read_csv
        
    Returns:
        DataFrame ou None se ocorrer erro
    """
    try:
        s3_client = get_s3_client()
        
        response = s3_client.get_object(
            Bucket=bucket,
            Key=key
        )
        
        content = response['Body'].read().decode('utf-8')
        return pd.read_csv(io.StringIO(content), **kwargs)
        
    except Exception as e:
        logging.error(f"Erro ao ler CSV {bucket}/{key}: {str(e)}")
        return None

def write_parquet_to_s3(df: pd.DataFrame, bucket: str, key: str) -> bool:
    """
    Escreve DataFrame como Parquet no S3.
    
    Args:
        df: DataFrame a ser salvo
        bucket: Nome do bucket
        key: Caminho/chave do arquivo
        
    Returns:
        True se operação for bem-sucedida, False caso contrário
    """
    try:
        s3_client = get_s3_client()
        
        buffer = io.BytesIO()
        df.to_parquet(buffer)
        buffer.seek(0)
        
        s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=buffer.getvalue()
        )
        
        logging.info(f"Arquivo salvo com sucesso: s3://{bucket}/{key}")
        return True
        
    except Exception as e:
        logging.error(f"Erro ao salvar parquet {bucket}/{key}: {str(e)}")
        return False

def write_csv_to_s3(df: pd.DataFrame, bucket: str, key: str, **kwargs) -> bool:
    """
    Escreve DataFrame como CSV no S3.
    
    Args:
        df: DataFrame a ser salvo
        bucket: Nome do bucket
        key: Caminho/chave do arquivo
        **kwargs: Argumentos adicionais para df.to_csv
        
    Returns:
        True se operação for bem-sucedida, False caso contrário
    """
    try:
        s3_client = get_s3_client()
        
        buffer = io.StringIO()
        df.to_csv(buffer, index=False, **kwargs)
        
        s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=buffer.getvalue().encode('utf-8')
        )
        
        logging.info(f"Arquivo salvo com sucesso: s3://{bucket}/{key}")
        return True
        
    except Exception as e:
        logging.error(f"Erro ao salvar CSV {bucket}/{key}: {str(e)}")
        return False

def get_s3_path_with_timestamp(base_path: str, extension: str = 'parquet') -> str:
    """
    Gera um caminho S3 com timestamp.
    
    Args:
        base_path: Caminho base
        extension: Extensão do arquivo
        
    Returns:
        Caminho com timestamp
    """
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    return f"{base_path}_{timestamp}.{extension}"

def s3_move_file(bucket: str, source_key: str, dest_key: str) -> bool:
    """
    Move/renomeia um arquivo no S3.
    
    Args:
        bucket: Nome do bucket
        source_key: Caminho atual
        dest_key: Novo caminho
        
    Returns:
        True se operação for bem-sucedida, False caso contrário
    """
    try:
        s3_client = get_s3_client()
        
        # Copia o arquivo
        s3_client.copy_object(
            Bucket=bucket,
            CopySource={'Bucket': bucket, 'Key': source_key},
            Key=dest_key
        )
        
        # Remove o original
        s3_client.delete_object(
            Bucket=bucket,
            Key=source_key
        )
        
        logging.info(f"Arquivo movido: s3://{bucket}/{source_key} -> s3://{bucket}/{dest_key}")
        return True
        
    except Exception as e:
        logging.error(f"Erro ao mover arquivo {source_key} para {dest_key}: {str(e)}")
        return False

def test_s3_connection(bucket: str) -> bool:
    """
    Testa conexão com bucket S3.
    
    Args:
        bucket: Nome do bucket a testar
        
    Returns:
        True se conexão funcionar, False caso contrário
    """
    try:
        s3_client = get_s3_client()
        
        # Tenta listar o conteúdo do bucket (limitado a 1 item)
        s3_client.list_objects_v2(
            Bucket=bucket,
            MaxKeys=1
        )
        
        return True
        
    except Exception as e:
        logging.error(f"Erro na conexão com bucket {bucket}: {str(e)}")
        return False