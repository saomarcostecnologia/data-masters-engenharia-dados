# src/utils/aws_utils.py
import boto3
import logging
from botocore.exceptions import ClientError
from typing import Any, Optional, List
import pandas as pd
from io import StringIO, BytesIO
import os
from datetime import datetime
from dotenv import load_dotenv

# Carrega variáveis de ambiente
load_dotenv()

class S3Handler:
    """Classe para gerenciar operações com AWS S3."""
    
    def __init__(self):
        """Inicializa conexão com AWS S3."""
        try:
            self.s3_client = boto3.client(
                's3',
                aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
                aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
                region_name=os.getenv('AWS_REGION')
            )
            self.bucket_name = os.getenv('AWS_BUCKET_NAME')
            logging.info(f"Conexão com S3 inicializada para o bucket: {self.bucket_name}")
        except Exception as e:
            logging.error(f"Erro ao conectar com S3: {str(e)}")
            raise

    def upload_dataframe(
        self,
        df: pd.DataFrame,
        file_path: str,
        layer: str = 'bronze',
        format: str = 'parquet'
    ) -> bool:
        """
        Faz upload de um DataFrame para o S3.
        
        Args:
            df: DataFrame a ser enviado
            file_path: Caminho do arquivo no S3 (sem extensão)
            layer: Camada de dados (bronze, silver, gold)
            format: Formato do arquivo (parquet, csv)
            
        Returns:
            bool: True se upload foi bem sucedido
        """
        try:
            # Adiciona timestamp ao nome do arquivo
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            file_name = f"{file_path}_{timestamp}"
            
            # Monta o caminho completo considerando a camada
            full_path = f"{layer}/{file_name}.{format}"
            
            # Prepara o arquivo com base no formato
            if format == 'parquet':
                buffer = BytesIO()
                df.to_parquet(buffer)
                buffer.seek(0)
                file_content = buffer.getvalue()
            else:  # csv
                buffer = StringIO()
                df.to_csv(buffer, index=False)
                file_content = buffer.getvalue().encode('utf-8')
            
            # Faz upload para S3
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=full_path,
                Body=file_content
            )
            
            logging.info(f"Upload realizado com sucesso: s3://{self.bucket_name}/{full_path}")
            return True
            
        except Exception as e:
            logging.error(f"Erro no upload para S3: {str(e)}")
            return False
    
    def download_file(
        self,
        file_path: str,
        format: str = None
    ) -> Optional[pd.DataFrame]:
        """
        Baixa arquivo do S3 e retorna como DataFrame.
        
        Args:
            file_path: Caminho completo do arquivo no S3 (incluindo camada)
            format: Formato do arquivo (inferido da extensão se None)
            
        Returns:
            Optional[pd.DataFrame]: DataFrame ou None se houver erro
        """
        try:
            # Infere o formato do arquivo se não for especificado
            if format is None:
                if file_path.endswith('.parquet'):
                    format = 'parquet'
                elif file_path.endswith('.csv'):
                    format = 'csv'
                else:
                    format = 'parquet'  # default
            
            response = self.s3_client.get_object(
                Bucket=self.bucket_name,
                Key=file_path
            )
            
            if format == 'parquet':
                buffer = BytesIO(response['Body'].read())
                return pd.read_parquet(buffer)
            else:  # csv
                content = response['Body'].read().decode('utf-8')
                return pd.read_csv(StringIO(content))
                
        except Exception as e:
            logging.error(f"Erro ao baixar arquivo do S3 ({file_path}): {str(e)}")
            return None

    def list_files(self, prefix: str = '') -> List[str]:
        """
        Lista arquivos no bucket S3.
        
        Args:
            prefix: Prefixo para filtrar arquivos (ex: 'bronze/')
            
        Returns:
            list: Lista de arquivos encontrados
        """
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=prefix
            )
            
            files = []
            if 'Contents' in response:
                files = [obj['Key'] for obj in response['Contents']]
                
            return files
            
        except Exception as e:
            logging.error(f"Erro ao listar arquivos com prefixo {prefix}: {str(e)}")
            return []

    def test_connection(self) -> bool:
        """
        Testa a conexão com o bucket S3.
        
        Returns:
            bool: True se a conexão está funcionando
        """
        try:
            # Tenta listar o conteúdo do bucket
            self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                MaxKeys=1
            )
            logging.info(f"Conexão com o bucket {self.bucket_name} testada com sucesso")
            return True
        except Exception as e:
            logging.error(f"Falha no teste de conexão com o bucket: {str(e)}")
            return False