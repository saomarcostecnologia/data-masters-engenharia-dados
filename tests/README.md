# Testes do Economic Indicators ETL

Este diretório contém os testes automatizados para o projeto Economic Indicators ETL.

## Estrutura

```
tests/
├── conftest.py            # Configurações e fixtures compartilhadas
├── integration/           # Testes de integração entre componentes
│   ├── test_api.py        # Testes de conectividade com APIs externas
│   ├── test_data_pipeline.py  # Testes do pipeline completo
│   ├── test_gold_transformation.py  # Testes da transformação Silver → Gold
│   ├── test_s3_connection.py  # Testes de conexão com S3
│   └── test_silver_transformation.py  # Testes da transformação Bronze → Silver
├── run_tests.py          # Script para execução dos testes
└── unit/                 # Testes unitários de componentes individuais
    ├── test_collectors/  # Testes dos coletores de dados
    │   ├── test_bcb_collector.py  # Testes do coletor do BCB
    │   ├── test_factory.py        # Testes da factory de coletores
    │   └── test_ibge_collector.py # Testes do coletor do IBGE
    ├── test_transformers/  # Testes dos transformadores de dados
    │   ├── test_bronze_to_silver.py  # Testes da transformação Bronze → Silver
    │   └── test_silver_to_gold.py    # Testes da transformação Silver → Gold
    └── test_utils/       # Testes dos utilitários
        ├── test_aws_utils.py  # Testes dos utilitários AWS
        └── test_error_handling.py  # Testes do sistema de tratamento de erros
```

## Tipos de Testes

### Testes Unitários (`unit/`)

Testam componentes individuais isoladamente, usando mocks para simular dependências externas.

- `test_collectors/`: Testes dos coletores de dados (BCB, IBGE)
- `test_transformers/`: Testes das transformações de dados (Bronze→Silver, Silver→Gold)
- `test_utils/`: Testes dos utilitários (AWS, tratamento de erros, etc.)

### Testes de Integração (`integration/`)

Testam a interação entre múltiplos componentes do sistema.

- `test_api.py`: Verifica a conectividade com as APIs externas
- `test_data_pipeline.py`: Testa o fluxo completo do pipeline de dados
- `test_s3_connection.py`: Testa a conexão com o serviço S3 da AWS
- `test_silver_transformation.py` e `test_gold_transformation.py`: Testam transformações com dados reais

## Como Executar os Testes

### Usando o Script `run_tests.py`

O script `run_tests.py` fornece uma interface de linha de comando para executar os testes:

```bash
# Executar todos os testes
python tests/run_tests.py --all

# Executar apenas testes unitários
python tests/run_tests.py --unit

# Executar apenas testes de integração
python tests/run_tests.py --integration

# Executar com relatório de cobertura
python tests/run_tests.py --all --cov

# Executar com saída detalhada
python tests/run_tests.py --all --verbose

# Gerar relatório HTML de cobertura
python tests/run_tests.py --all --cov --html
```

### Usando Pytest Diretamente

Você também pode executar os testes diretamente com pytest:

```bash
# Executar todos os testes
pytest tests/

# Executar testes unitários
pytest tests/unit/

# Executar testes de integração
pytest tests/integration/

# Executar com verbosidade
pytest -v tests/

# Executar com cobertura
pytest --cov=src tests/
```

## Fixture para Testes

O arquivo `conftest.py` contém fixtures compartilhadas úteis para os testes:

- `mock_environment`: Configura variáveis de ambiente para testes
- `mock_aws`: Mock para serviços AWS
- `s3_handler`: Instância de S3Handler configurada para testes
- `bcb_collector`: Instância de BCBCollector para testes
- `ibge_collector`: Instância de IBGECollector para testes
- `collector_factory`: Factory de coletores para testes
- Conjuntos de dados de exemplo (IPCA, SELIC, PIB, etc.)
- `setup_test_data`: Configura dados de teste no bucket S3 mock

## Adicionando Novos Testes

Ao adicionar novos testes:

1. Siga a convenção de nomenclatura: arquivos começam com `test_` e funções de teste também
2. Coloque testes unitários na pasta `unit/` e testes de integração na pasta `integration/`
3. Utilize as fixtures existentes em `conftest.py` quando apropriado
4. Para testes de recursos AWS, use `mock_aws` para evitar acessar serviços reais durante testes
5. Para novos módulos, crie arquivos de teste com estrutura similar aos existentes