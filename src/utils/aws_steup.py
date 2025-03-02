# src/utils/aws_utils.py
import boto3
import logging
from botocore.exceptions import ClientError
from typing import Any, Optional, List, Dict, Union
import pandas as pd
from io import StringIO, BytesIO
import os
from datetime import datetime
from dotenv import load_dotenv

# Carrega variáveis de ambiente
load_dotenv()

class S3Handler:
    """Classe para gerenciar operações com AWS S3."""
    
    def __init__(self, bucket_name=None, region=None):
        """
        Inicializa conexão com AWS S3.
        
        Args:
            bucket_name: Nome do bucket (se None, usa o valor de AWS_BUCKET_NAME)
            region: Região AWS (se None, usa o valor de AWS_REGION)
        """
        try:
            self.s3_client = boto3.client(
                's3',
                aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
                aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
                region_name=region or os.getenv('AWS_REGION')
            )
            self.bucket_name = bucket_name or os.getenv('AWS_BUCKET_NAME')
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
    
    # Métodos incorporados de aws_helpers.py
    
    def get_latest_file(self, prefix: str) -> Optional[str]:
        """
        Obtém o arquivo mais recente em um caminho do S3.
        
        Args:
            prefix: Prefixo/caminho dos arquivos
            
        Returns:
            Path do arquivo mais recente ou None
        """
        try:
            files = self.list_files(prefix)
            
            if not files:
                logging.warning(f"Nenhum arquivo encontrado em {self.bucket_name}/{prefix}")
                return None
                
            # Ordena para pegar o mais recente (presumindo nomeação cronológica)
            return sorted(files)[-1]
            
        except Exception as e:
            logging.error(f"Erro ao obter arquivo mais recente: {str(e)}")
            return None
    
    def read_parquet(self, key: str) -> Optional[pd.DataFrame]:
        """
        Lê arquivo Parquet do S3 como DataFrame.
        
        Args:
            key: Caminho/chave do arquivo
            
        Returns:
            DataFrame ou None se ocorrer erro
        """
        return self.download_file(key, format='parquet')
    
    def read_csv(self, key: str, **kwargs) -> Optional[pd.DataFrame]:
        """
        Lê arquivo CSV do S3 como DataFrame.
        
        Args:
            key: Caminho/chave do arquivo
            **kwargs: Argumentos adicionais para pd.read_csv
            
        Returns:
            DataFrame ou None se ocorrer erro
        """
        try:
            response = self.s3_client.get_object(
                Bucket=self.bucket_name,
                Key=key
            )
            
            content = response['Body'].read().decode('utf-8')
            return pd.read_csv(StringIO(content), **kwargs)
            
        except Exception as e:
            logging.error(f"Erro ao ler CSV {self.bucket_name}/{key}: {str(e)}")
            return None
    
    def write_parquet(self, df: pd.DataFrame, key: str) -> bool:
        """
        Escreve DataFrame como Parquet no S3.
        
        Args:
            df: DataFrame a ser salvo
            key: Caminho/chave do arquivo
            
        Returns:
            True se operação for bem-sucedida, False caso contrário
        """
        try:
            buffer = BytesIO()
            df.to_parquet(buffer)
            buffer.seek(0)
            
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=key,
                Body=buffer.getvalue()
            )
            
            logging.info(f"Arquivo salvo com sucesso: s3://{self.bucket_name}/{key}")
            return True
            
        except Exception as e:
            logging.error(f"Erro ao salvar parquet {self.bucket_name}/{key}: {str(e)}")
            return False
    
    def write_csv(self, df: pd.DataFrame, key: str, **kwargs) -> bool:
        """
        Escreve DataFrame como CSV no S3.
        
        Args:
            df: DataFrame a ser salvo
            key: Caminho/chave do arquivo
            **kwargs: Argumentos adicionais para df.to_csv
            
        Returns:
            True se operação for bem-sucedida, False caso contrário
        """
        try:
            buffer = StringIO()
            df.to_csv(buffer, index=False, **kwargs)
            
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=key,
                Body=buffer.getvalue().encode('utf-8')
            )
            
            logging.info(f"Arquivo salvo com sucesso: s3://{self.bucket_name}/{key}")
            return True
            
        except Exception as e:
            logging.error(f"Erro ao salvar CSV {self.bucket_name}/{key}: {str(e)}")
            return False
    
    def get_path_with_timestamp(self, base_path: str, extension: str = 'parquet') -> str:
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
    
    def move_file(self, source_key: str, dest_key: str) -> bool:
        """
        Move/renomeia um arquivo no S3.
        
        Args:
            source_key: Caminho atual
            dest_key: Novo caminho
            
        Returns:
            True se operação for bem-sucedida, False caso contrário
        """
        try:
            # Copia o arquivo
            self.s3_client.copy_object(
                Bucket=self.bucket_name,
                CopySource={'Bucket': self.bucket_name, 'Key': source_key},
                Key=dest_key
            )
            
            # Remove o original
            self.s3_client.delete_object(
                Bucket=self.bucket_name,
                Key=source_key
            )
            
            logging.info(f"Arquivo movido: s3://{self.bucket_name}/{source_key} -> s3://{self.bucket_name}/{dest_key}")
            return True
            
        except Exception as e:
            logging.error(f"Erro ao mover arquivo {source_key} para {dest_key}: {str(e)}")
            return False