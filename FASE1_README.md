# Fase 1: Refatoração e Expansão do Pipeline ETL

## Resumo das Melhorias Implementadas

Nesta primeira fase da evolução do nosso pipeline de indicadores econômicos, implementamos as seguintes melhorias:

1. **Refatoração para Design Patterns**:
   - **Abstract Factory**: Para instanciação de coletores de diferentes fontes
   - **Template Method**: Para padronizar o fluxo de coleta de dados
   - **Command**: Para encapsular operações de coleta
   - **Strategy**: Para diferentes estratégias de coleta por fonte

2. **Expansão de Fontes de Dados**:
   - **IBGE Collector**: Novo coletor para dados do IBGE (IPCA-15, INPC, PNAD, PIB)

3. **Preparação para Processamento Distribuído**:
   - **Spark Transformer**: Adaptação do transformador Bronze→Silver para PySpark

## Estrutura dos Arquivos

```
src/
│
├── collectors/
│   ├── abstract_collector.py   # Interface abstrata (Template Method)
│   ├── base_collector.py       # Implementação base
│   ├── bcb_collector.py        # Coletor do Banco Central (refatorado)
│   ├── ibge_collector.py       # Novo coletor para o IBGE
│   └── factory.py              # Factory para criar coletores
│
├── transformers/
│   └── bronze_to_silver_spark.py  # Versão Spark do transformador
│
scripts/
│
├── collect_economic_data.py    # Script para coletar dados
└── run_spark_transformation.py # Script para testar transformações Spark
```

## Como Testar as Novas Funcionalidades

### 1. Requisitos

Verifique se você tem instalado:
- Python 3.11+
- PySpark 3.3+ (para testes do transformador Spark)
- Pandas, requests e demais dependências do `requirements.txt`

Instale as dependências com:
```bash
pip install -r requirements.txt
```

### 2. Coletando Dados de Múltiplas Fontes

O script `collect_economic_data.py` permite coletar dados de diferentes fontes.

#### Listando indicadores disponíveis:
```bash
python scripts/collect_economic_data.py --list
```

#### Coletando dados do BCB:
```bash
python scripts/collect_economic_data.py --source bcb --indicators ipca,selic --months 6
```

#### Coletando dados do IBGE:
```bash
python scripts/collect_economic_data.py --source ibge --indicators ipca15,inpc --months 12
```

#### Coletando todos os indicadores de todas as fontes:
```bash
python scripts/collect_economic_data.py --source all --indicators all
```

### 3. Testando o Transformador Spark

O script `run_spark_transformation.py` permite testar as transformações Spark localmente.

#### Executando com dados de exemplo:
```bash
python scripts/run_spark_transformation.py --indicator ipca --local-mode --output-path ./output/ipca_spark
```

#### Testando outro indicador:
```bash
python scripts/run_spark_transformation.py --indicator selic --local-mode
```

## Próximos Passos

Na próxima fase, vamos focar em:

1. **AWS Step Functions para Orquestração**:
   - Implementar fluxos de trabalho usando AWS Step Functions
   - Criar funções Lambda para processamento de eventos

2. **Infraestrutura como Código**:
   - Definir recursos AWS com CloudFormation ou Terraform

3. **Integração EMR e Glue**:
   - Configurar processamento Spark em escala usando AWS EMR ou AWS Glue

## Observações

- A configuração do S3 deve estar corretamente definida no arquivo `.env` para que a coleta e armazenamento de dados funcionem
- Os transformadores Spark foram refatorados para manter a mesma lógica de negócio, mas utilizando o paradigma de processamento distribuído
- O código segue princípios SOLID e padrões de design para facilitar manutenção e extensão futura