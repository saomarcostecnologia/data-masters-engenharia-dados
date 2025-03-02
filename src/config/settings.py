# src/config/settings.py
"""Gerenciador de configurações centralizado."""

import os
import yaml
from dotenv import load_dotenv

# Carrega variáveis de ambiente de .env
load_dotenv()

class Settings:
    """Classe para gerenciar configurações do projeto."""
    
    def __init__(self, config_file=None):
        """
        Inicializa configurações, com prioridade:
        1. Variáveis de ambiente
        2. Arquivo de configuração YAML
        3. Valores padrão
        """
        self.config = {
            # AWS
            'aws_region': 'us-east-1',
            'aws_profile': None,
            'data_lake_bucket': 'economic-indicators-data-lake',
            
            # Projeto
            'project_name': 'economic-indicators-etl',
            'environment': 'dev',
            
            # Configurações de dados
            'default_start_date': '2020-01-01',
            'default_data_sources': ['bcb', 'ibge'],
            
            # Logs
            'log_level': 'INFO',
            'log_to_file': False,
            'log_file': 'logs/app.log'
        }
        
        # Carrega de arquivo YAML se especificado
        if config_file and os.path.exists(config_file):
            with open(config_file, 'r') as f:
                yaml_config = yaml.safe_load(f)
                if yaml_config:
                    self.config.update(yaml_config)
        
        # Sobrescreve com variáveis de ambiente
        self._override_from_env()
        
    def _override_from_env(self):
        """Sobrescreve configurações com variáveis de ambiente."""
        env_map = {
            'AWS_REGION': 'aws_region',
            'AWS_PROFILE': 'aws_profile',
            'AWS_BUCKET_NAME': 'data_lake_bucket',
            'PROJECT_NAME': 'project_name',
            'ENVIRONMENT': 'environment',
            'LOG_LEVEL': 'log_level'
        }
        
        for env_var, config_key in env_map.items():
            if env_var in os.environ:
                self.config[config_key] = os.environ[env_var]
    
    def get(self, key, default=None):
        """Obtém uma configuração."""
        return self.config.get(key, default)
    
    def set(self, key, value):
        """Define uma configuração."""
        self.config[key] = value
        
    def get_config_dict(self):
        """Retorna todas as configurações como dicionário."""
        return self.config.copy()