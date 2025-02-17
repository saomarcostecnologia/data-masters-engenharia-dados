# setup_project.py
import os

def create_directory_structure():
    """Cria a estrutura de diretórios do projeto."""
    
    directories = [
        'src/collectors',
        'src/transformers',
        'src/utils',
        'src/config',
        'tests/test_collectors',
        'tests/test_transformers',
        'tests/test_utils',
        'docs',
        'dags',
        '.github/workflows'
    ]
    
    # Cria diretórios
    for directory in directories:
        os.makedirs(directory, exist_ok=True)
        # Cria __init__.py em pacotes Python
        if directory.startswith(('src/', 'tests/')):
            with open(os.path.join(directory, '__init__.py'), 'w') as f:
                pass
    
    # Cria arquivos básicos
    files = {
        'requirements.txt': '''pandas==2.1.0
requests==2.31.0
python-dotenv==1.0.0
boto3==1.28.44
pytest==7.4.2
black==23.7.0
flake8==6.1.0
mypy==1.5.1''',
        
        'README.md': '''# Economic Indicators ETL
        
Projeto de pipeline de dados para indicadores econômicos e sociais.

## Setup
1. Criar ambiente virtual: `python -m venv venv`
2. Ativar ambiente: `source venv/bin/activate` (Linux/Mac) ou `venv\\Scripts\\activate` (Windows)
3. Instalar dependências: `pip install -r requirements.txt`

## Estrutura
- `src/`: Código fonte
- `tests/`: Testes unitários
- `docs/`: Documentação
- `dags/`: DAGs do Airflow''',
        
        '.gitignore': '''venv/
__pycache__/
*.pyc
.env
.pytest_cache/
.coverage
*.log''',
        
        '.env.example': '''# Credenciais AWS
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
AWS_REGION=your_region

# Configurações do Projeto
ENVIRONMENT=development
LOG_LEVEL=INFO'''
    }
    
    for file_path, content in files.items():
        with open(file_path, 'w') as f:
            f.write(content)

if __name__ == "__main__":
    create_directory_structure()
    print("Estrutura do projeto criada com sucesso!")