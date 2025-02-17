# test_s3_connection.py
import logging
from src.utils.aws_utils import S3Handler

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def main():
    """Testa a conexão com o S3 e lista os arquivos existentes."""
    
    try:
        # Inicializa o handler do S3
        s3_handler = S3Handler()
        
        # Testa a conexão
        if s3_handler.test_connection():
            print(f"✅ Conexão com bucket '{s3_handler.bucket_name}' bem sucedida!")
        else:
            print(f"❌ Falha na conexão com o bucket '{s3_handler.bucket_name}'")
            return
        
        # Lista arquivos em cada camada
        camadas = ['bronze', 'silver', 'gold']
        for camada in camadas:
            print(f"\nArquivos na camada '{camada}':")
            arquivos = s3_handler.list_files(prefix=f"{camada}/")
            
            if arquivos:
                for arquivo in arquivos:
                    print(f"  - {arquivo}")
            else:
                print(f"  Nenhum arquivo encontrado na camada {camada}")
                
    except Exception as e:
        print(f"Erro durante o teste: {str(e)}")

if __name__ == "__main__":
    main()