# Economic Indicators ETL Pipeline MPV

## 📊 Sobre o Projeto
Pipeline de dados para coleta, processamento e análise de indicadores econômicos brasileiros, utilizando arquitetura em camadas (Lakehouse) com AWS S3.

## 🏗️ Arquitetura
- **Camada Bronze**: Dados brutos coletados das APIs
- **Camada Silver**: Dados limpos e padronizados
- **Camada Gold**: Dados agregados e análises

## 🔧 Tecnologias Utilizadas
- Python 3.11
- AWS S3
- Pandas
- Requests
- PyArrow
- Boto3
- Python-dotenv

## 📁 Estrutura do Projeto
```
economic-indicators-etl/
├── src/
│   ├── collectors/         # Coletores de dados
│   ├── transformers/      # Transformações de dados
│   ├── utils/             # Utilitários
│   └── config/           # Configurações
├── tests/                 # Testes unitários
├── docs/                 # Documentação
└── requirements.txt      # Dependências
```

## 🚀 Como Executar
1. Clone o repositório
```bash
git clone https://github.com/seu-usuario/economic-indicators-etl.git
cd economic-indicators-etl
```

2. Crie e ative o ambiente virtual
```bash
python -m venv venv
# Windows
venv\Scripts\activate
# Linux/Mac
source venv/bin/activate
```

3. Instale as dependências
```bash
pip install -r requirements.txt
```

4. Configure as variáveis de ambiente
- Crie um arquivo `.env` baseado no `.env.example`
- Adicione suas credenciais AWS

5. Execute o coletor
```bash
python -m src.collectors.bcb_collector
```

## 📊 Fontes de Dados
- Banco Central do Brasil (BCB)
  - IPCA (Inflação)
  - SELIC (Taxa de Juros)
  - PIB
  - Taxa de Câmbio
  - Taxa de Desemprego

## 🔜 Próximos Passos
- [ ] Implementar transformações para camada Silver
- [ ] Criar agregações para camada Gold
- [ ] Adicionar testes unitários
- [ ] Implementar mais fontes de dados
- [ ] Configurar CI/CD

## 🤝 Contribuições
Contribuições são bem-vindas! Por favor, leia as diretrizes de contribuição antes de submeter um Pull Request.

## 📝 Licença
Este projeto está sob a licença MIT.


Copy# Economic Indicators ETL

## 📊 Sobre o Projeto
Pipeline de dados para coleta, processamento e análise de indicadores econômicos, utilizando arquitetura em camadas (Lakehouse) com AWS S3.

## 🚀 Como Começar

### Pré-requisitos
- Python 3.11+
- Conta AWS e credenciais configuradas (AWS CLI ou variáveis de ambiente)
- Pip (gerenciador de pacotes Python)

### Instalação

1. Clone o repositório
```bash
git clone https://github.com/seu-usuario/economic-indicators-etl.git
cd economic-indicators-etl