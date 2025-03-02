# src/utils/helpers/aws_utils.py
"""Helpers específicos para interação com serviços AWS."""

import boto3
import logging
from botocore.exceptions import ClientError
from typing import Dict, List, Optional, Any
import json
import os

def get_aws_session(profile_name=None, region=None):
    """
    Cria uma sessão AWS com as credenciais fornecidas ou padrão.
    
    Args:
        profile_name: Nome do perfil de credenciais (opcional)
        region: Região AWS (opcional)
        
    Returns:
        boto3.Session: Sessão AWS configurada
    """
    try:
        session = boto3.Session(profile_name=profile_name, region_name=region)
        return session
    except Exception as e:
        logging.error(f"Erro ao criar sessão AWS: {str(e)}")
        raise

def create_s3_client(session=None, region=None):
    """
    Cria um cliente S3 usando uma sessão AWS existente ou nova.
    
    Args:
        session: Sessão AWS existente (opcional)
        region: Região AWS (opcional)
        
    Returns:
        boto3.client: Cliente S3 configurado
    """
    try:
        if session:
            return session.client('s3', region_name=region)
        else:
            return boto3.client('s3', region_name=region)
    except Exception as e:
        logging.error(f"Erro ao criar cliente S3: {str(e)}")
        raise

def create_lambda_client(session=None, region=None):
    """
    Cria um cliente Lambda usando uma sessão AWS existente ou nova.
    
    Args:
        session: Sessão AWS existente (opcional)
        region: Região AWS (opcional)
        
    Returns:
        boto3.client: Cliente Lambda configurado
    """
    try:
        if session:
            return session.client('lambda', region_name=region)
        else:
            return boto3.client('lambda', region_name=region)
    except Exception as e:
        logging.error(f"Erro ao criar cliente Lambda: {str(e)}")
        raise

# Funções para buckets S3
def check_bucket_exists(bucket_name, s3_client=None):
    """
    Verifica se um bucket S3 existe.
    
    Args:
        bucket_name: Nome do bucket
        s3_client: Cliente S3 (opcional)
        
    Returns:
        bool: True se o bucket existe
    """
    if not s3_client:
        s3_client = create_s3_client()
        
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        return True
    except ClientError:
        return False

def create_bucket_if_not_exists(bucket_name, region=None, s3_client=None):
    """
    Cria um bucket S3 se ele não existir.
    
    Args:
        bucket_name: Nome do bucket
        region: Região AWS (opcional)
        s3_client: Cliente S3 (opcional)
        
    Returns:
        bool: True se criado ou já existente
    """
    if not s3_client:
        s3_client = create_s3_client(region=region)
        
    if check_bucket_exists(bucket_name, s3_client):
        logging.info(f"Bucket {bucket_name} já existe")
        return True
        
    try:
        if region and region != 'us-east-1':
            s3_client.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={'LocationConstraint': region}
            )
        else:
            # Para us-east-1, não se especifica a configuração de localização
            s3_client.create_bucket(Bucket=bucket_name)
            
        logging.info(f"Bucket {bucket_name} criado com sucesso")
        return True
        
    except ClientError as e:
        logging.error(f"Erro ao criar bucket {bucket_name}: {str(e)}")
        return False

# E outras funções úteis...