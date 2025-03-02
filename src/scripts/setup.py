# scripts/setup.py
"""Script de configuração inicial do projeto."""

import os
import sys
import argparse
import subprocess
import yaml
import getpass

def create_config_file(config_path):
    """Cria arquivo de configuração interativamente."""
    config = {}
    
    print("\nConfiguração do Projeto Economic Indicators ETL")
    print("==============================================")
    
    # AWS
    print("\n== Configurações AWS ==")
    config['aws_region'] = input("Região AWS [us-east-1]: ") or "us-east-1"
    config['aws_profile'] = input("Perfil AWS (deixe vazio para usar padrão): ")
    if not config['aws_profile']:
        config['aws_profile'] = None
        
    # Defina um nome de bucket único para o usuário
    default_bucket = f"economic-indicators-{getpass.getuser().lower()}-{config['aws_region']}"
    config['data_lake_bucket'] = input(f"Nome do bucket S3 para Data Lake [{default_bucket}]: ") or default_bucket
    
    # Projeto
    print("\n== Configurações do Projeto ==")
    config['project_name'] = input("Nome do projeto [economic-indicators-etl]: ") or "economic-indicators-etl"
    config['environment'] = input("Ambiente (dev, staging, prod) [dev]: ") or "dev"
    
    # Logs
    print("\n== Configurações de Logging ==")
    config['log_level'] = input("Nível de log (INFO, DEBUG, WARNING) [INFO]: ") or "INFO"
    log_to_file = input("Salvar logs em arquivo? (s/n) [n]: ").lower()
    config['log_to_file'] = log_to_file == 's'
    if config['log_to_file']:
        config['log_file'] = input("Caminho do arquivo de log [logs/app.log]: ") or "logs/app.log"
    
    # Salva a configuração
    os.makedirs(os.path.dirname(config_path), exist_ok=True)
    with open(config_path, 'w') as f:
        yaml.dump(config, f, default_flow_style=False)
    
    print(f"\nConfigurações salvas em {config_path}")
    
    # Cria também um .env com as mesmas configurações
    with open('.env', 'w') as f:
        f.write(f"AWS_REGION={config['aws_region']}\n")
        if config['aws_profile']:
            f.write(f"AWS_PROFILE={config['aws_profile']}\n")
        f.write(f"AWS_BUCKET_NAME={config['data_lake_bucket']}\n")
        f.write(f"PROJECT_NAME={config['project_name']}\n")
        f.write(f"ENVIRONMENT={config['environment']}\n")
        f.write(f"LOG_LEVEL={config['log_level']}\n")
    
    print("Variáveis de ambiente também salvas em .env")
    
    return config

def setup_aws_resources(config):
    """Configura recursos AWS iniciais."""
    try:
        # Importa funções de AWS
        sys.path.append(os.path.abspath(os.path.dirname(os.path.dirname(__file__))))
        from src.utils.helpers.aws_utils import create_s3_client, create_bucket_if_not_exists
        
        print("\nConfiguração de recursos AWS")
        print("===========================")
        
        # Cria cliente S3
        profile = config.get('aws_profile')
        region = config.get('aws_region')
        
        print(f"Usando região: {region}")
        if profile:
            print(f"Usando perfil: {profile}")
            
        s3_client = create_s3_client(region=region)
        
        # Cria bucket S3
        bucket_name = config.get('data_lake_bucket')
        if create_bucket_if_not_exists(bucket_name, region, s3_client):
            print(f"✅ Bucket {bucket_name} verificado/criado com sucesso!")
            
            # Cria estrutura de diretórios no bucket
            for prefix in ['bronze/', 'silver/', 'gold/', 'scripts/']:
                s3_client.put_object(Bucket=bucket_name, Key=prefix)
                
            print("✅ Estrutura de diretórios criada no bucket")
            return True
        else:
            print(f"❌ Falha ao criar bucket {bucket_name}")
            return False
            
    except Exception as e:
        print(f"❌ Erro ao configurar recursos AWS: {str(e)}")
        return False

def main():
    """Função principal de setup."""
    parser = argparse.ArgumentParser(description="Configuração inicial do projeto Economic Indicators ETL")
    parser.add_argument('--config', default='config/settings.yaml', help='Caminho para salvar arquivo de configuração')
    parser.add_argument('--skip-aws', action='store_true', help='Pular criação de recursos AWS')
    
    args = parser.parse_args()
    
    # Cria diretórios do projeto
    for directory in ['logs', 'data', 'config']:
        os.makedirs(directory, exist_ok=True)
    
    # Executa setup
    config = create_config_file(args.config)
    
    if not args.skip_aws:
        if setup_aws_resources(config):
            print("\n✅ Setup dos recursos AWS concluído com sucesso!")
        else:
            print("\n❌ Setup dos recursos AWS falhou")
    
    # Instala dependências
    print("\nInstalando dependências do projeto...")
    subprocess.run([sys.executable, "-m", "pip", "install", "-r", "requirements.txt"])
    
    print("\n✅ Setup concluído! Agora você pode executar o pipeline ETL.")
    print("\nPara coletar dados: python -m src.scripts.collect_data")

if __name__ == "__main__":
    main()