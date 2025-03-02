"""Testes de integração com o serviço S3 da AWS."""

import pytest
import logging
from src.utils.aws_utils import S3Handler

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def test_s3_connection():
    """Testa a conexão com o S3 e lista os arquivos existentes."""
    
    try:
        # Inicializa o handler do S3
        s3_handler = S3Handler()
        
        # Testa a conexão
        assert s3_handler.test_connection() is True
        print(f"✅ Conexão com bucket '{s3_handler.bucket_name}' bem sucedida!")
        
        return True
        
    except Exception as e:
        print(f"Erro durante o teste: {str(e)}")
        pytest.fail(f"Falha na conexão com S3: {str(e)}")

def test_s3_list_files():
    """Testa a listagem de arquivos em cada camada do S3."""
    
    try:
        # Inicializa o handler do S3
        s3_handler = S3Handler()
        
        # Lista arquivos em cada camada
        camadas = ['bronze', 'silver', 'gold']
        
        for camada in camadas:
            print(f"\nArquivos na camada '{camada}':")
            arquivos = s3_handler.list_files(prefix=f"{camada}/")
            
            # Verificação básica - não garante que existam arquivos, apenas que a operação funcionou
            assert isinstance(arquivos, list)
            
            if arquivos:
                for arquivo in arquivos:
                    print(f"  - {arquivo}")
            else:
                print(f"  Nenhum arquivo encontrado na camada {camada}")
                
        return True
        
    except Exception as e:
        print(f"Erro ao listar arquivos: {str(e)}")
        pytest.fail(f"Falha ao listar arquivos no S3: {str(e)}")

if __name__ == "__main__":
    test_s3_connection()
    test_s3_list_files()