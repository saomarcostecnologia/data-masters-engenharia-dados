# src/utils/helpers/aws_helpers.py
"""
Arquivo de compatibilidade para funções obsoletas.
Redireciona para as implementações atualizadas no S3Handler.
IMPORTANTE: Este arquivo é SOMENTE para compatibilidade temporária.
Por favor atualize seus imports para usar S3Handler diretamente.
"""

import warnings
from ..aws_utils import S3Handler

# Criamos uma instância compartilhada
_s3_handler = S3Handler()

# Emitimos um aviso de depreciação
warnings.warn(
    "O módulo aws_helpers está obsoleto. Por favor, atualize seu código para usar S3Handler diretamente.",
    DeprecationWarning, 
    stacklevel=2
)

# Funções redirecionadas
def get_s3_client():
    """Obtém um cliente S3. Obsoleto: Use S3Handler()."""
    return _s3_handler.s3_client

def list_s3_files(bucket, prefix=''):
    """Lista arquivos no S3. Obsoleto: Use S3Handler().list_files()."""
    return _s3_handler.list_files(prefix)

def get_latest_s3_file(bucket, prefix):
    """Obtém arquivo mais recente. Obsoleto: Use S3Handler().get_latest_file()."""
    return _s3_handler.get_latest_file(prefix)

def read_parquet_from_s3(bucket, key):
    """Lê arquivo Parquet do S3. Obsoleto: Use S3Handler().read_parquet()."""
    return _s3_handler.read_parquet(key)

def read_csv_from_s3(bucket, key, **kwargs):
    """Lê arquivo CSV do S3. Obsoleto: Use S3Handler().read_csv()."""
    return _s3_handler.read_csv(key, **kwargs)

def write_parquet_to_s3(df, bucket, key):
    """Escreve DataFrame como Parquet. Obsoleto: Use S3Handler().write_parquet()."""
    return _s3_handler.write_parquet(df, key)

def write_csv_to_s3(df, bucket, key, **kwargs):
    """Escreve DataFrame como CSV. Obsoleto: Use S3Handler().write_csv()."""
    return _s3_handler.write_csv(df, key, **kwargs)

def get_s3_path_with_timestamp(base_path, extension='parquet'):
    """Gera caminho com timestamp. Obsoleto: Use S3Handler().get_path_with_timestamp()."""
    return _s3_handler.get_path_with_timestamp(base_path, extension)

def s3_move_file(bucket, source_key, dest_key):
    """Move arquivo no S3. Obsoleto: Use S3Handler().move_file()."""
    return _s3_handler.move_file(source_key, dest_key)

def test_s3_connection(bucket):
    """Testa conexão com S3. Obsoleto: Use S3Handler().test_connection()."""
    return _s3_handler.test_connection()