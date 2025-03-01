# tests/integration/test_gold_transformation.py
import sys
import os
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path

# Adiciona o diretório raiz ao path para importar módulos do projeto
project_root = str(Path(__file__).parent.parent.parent)
sys.path.append(project_root)

from src.transformers.silver_to_gold import EconomicIndicatorsGoldTransformer
from src.utils.helpers.logging_utils import get_logger, log_execution_time, log_dataframe_stats
from src.utils.aws_utils import S3Handler

# Configuração de logging
logger = get_logger("test_gold_transformation")

@log_execution_time(logger=logger, operation_name="Teste de Transformação Gold")
def test_gold_transformation():
    """Testa a transformação de dados da camada silver para gold."""
    
    try:
        # Inicializa o transformador
        transformer = EconomicIndicatorsGoldTransformer()
        
        # Testa a conexão S3
        if not transformer.s3_handler.test_connection():
            logger.error("❌ Falha na conexão com o S3. Verifique suas credenciais.")
            return False
            
        logger.info("✅ Conexão com S3 estabelecida com sucesso!")
        
        # Inicia o processamento da camada gold
        logger.info("Iniciando processamento da camada gold...")
        success = transformer.process_gold_layer()
        
        if success:
            logger.info("✅ Transformação para camada gold concluída com sucesso!")
        else:
            logger.error("❌ Falha na transformação para camada gold.")
        
        return success
        
    except Exception as e:
        logger.error(f"❌ Erro durante o teste: {str(e)}")
        return False

@log_execution_time(logger=logger, operation_name="Validação de Dashboards Gold")
def validate_gold_dashboards():
    """Valida os dashboards criados na camada gold."""
    
    try:
        # Inicializa S3Handler
        s3_handler = S3Handler()
        
        # Lista os arquivos na camada gold
        gold_files = s3_handler.list_files(prefix="gold/dashboards")
        
        if not gold_files:
            logger.warning("Nenhum arquivo encontrado na camada gold para validação")
            return False
            
        logger.info(f"Encontrados {len(gold_files)} dashboards na camada gold")
            
        # Dashboards esperados
        expected_dashboards = [
            'monthly_indicators',
            'labor_market',
            'macro_dashboard'
        ]
        
        # Verifica cada dashboard
        dashboard_status = {}
        
        for dashboard in expected_dashboards:
            # Filtra arquivos para este dashboard
            db_files = [f for f in gold_files if dashboard in f]
            
            if not db_files:
                logger.warning(f"Dashboard {dashboard} não encontrado")
                dashboard_status[dashboard] = False
                continue
                
            # Pega o arquivo mais recente
            latest_file = sorted(db_files)[-1]
            logger.info(f"Validando dashboard: {dashboard} (arquivo: {latest_file})")
            
            # Carrega o arquivo
            df = s3_handler.download_file(latest_file)
            
            if df is None or df.empty:
                logger.error(f"❌ Não foi possível carregar o dashboard {dashboard}")
                dashboard_status[dashboard] = False
                continue
                
            # Loga estatísticas
            log_dataframe_stats(df, logger, f"Dashboard {dashboard}")
            
            # Validações específicas por dashboard
            if dashboard == 'monthly_indicators':
                valid = validate_monthly_indicators(df)
            elif dashboard == 'labor_market':
                valid = validate_labor_market(df)
            elif dashboard == 'macro_dashboard':
                valid = validate_macro_dashboard(df)
            else:
                valid = True  # Por padrão, consideramos válido
                
            dashboard_status[dashboard] = valid
            
            if valid:
                logger.info(f"✅ Dashboard {dashboard} validado com sucesso")
            else:
                logger.error(f"❌ Dashboard {dashboard} inválido")
        
        # Retorna True se todos os dashboards esperados existirem e forem válidos
        all_valid = all(dashboard_status.values()) if dashboard_status else False
        
        logger.info(f"Validação de dashboards concluída: {all_valid}")
        return all_valid
        
    except Exception as e:
        logger.error(f"❌ Erro durante validação de dashboards: {str(e)}")
        return False

def validate_monthly_indicators(df):
    """Valida o dashboard de indicadores mensais."""
    
    # Colunas obrigatórias
    required_cols = ['year_month', 'ipca', 'selic', 'cambio']
    
    # Verifica se as colunas existem
    missing_cols = [col for col in required_cols if col not in df.columns]
    
    if missing_cols:
        logger.error(f"Colunas ausentes no dashboard mensal: {missing_cols}")
        return False
        
    # Verifica se há pelo menos 3 registros (meses)
    if len(df) < 3:
        logger.warning(f"Poucos registros no dashboard mensal: {len(df)}")
        
    # Verifica se o índice de pressão econômica foi calculado
    if 'economic_pressure_index' not in df.columns:
        logger.warning("Índice de pressão econômica não encontrado no dashboard mensal")
        
    # Verifica valores nulos
    null_counts = df[required_cols].isnull().sum()
    if null_counts.sum() > 0:
        logger.warning(f"Valores nulos encontrados no dashboard mensal: {null_counts.to_dict()}")
        
    return True

def validate_labor_market(df):
    """Valida o dashboard do mercado de trabalho."""
    
    # Colunas obrigatórias
    required_cols = ['date', 'unemployment_rate']
    
    # Verifica se as colunas existem
    missing_cols = [col for col in required_cols if col not in df.columns]
    
    if missing_cols:
        logger.error(f"Colunas ausentes no dashboard do mercado de trabalho: {missing_cols}")
        return False
        
    # Verifica se há pelo menos 3 registros
    if len(df) < 3:
        logger.warning(f"Poucos registros no dashboard do mercado de trabalho: {len(df)}")
        
    return True

def validate_macro_dashboard(df):
    """Valida o dashboard macroeconômico."""
    
    # Colunas obrigatórias
    required_cols = ['indicator', 'last_value', 'last_date']
    
    # Verifica se as colunas existem
    missing_cols = [col for col in required_cols if col not in df.columns]
    
    if missing_cols:
        logger.error(f"Colunas ausentes no dashboard macroeconômico: {missing_cols}")
        return False
        
    # Verifica se contém todos os indicadores
    indicators = ['ipca', 'selic', 'cambio', 'desemprego']
    missing_indicators = [ind for ind in indicators if ind not in df['indicator'].values]
    
    if missing_indicators:
        logger.warning(f"Indicadores ausentes no dashboard macroeconômico: {missing_indicators}")
        
    # Verifica se o índice de saúde econômica está presente
    if 'economic_health' not in df['indicator'].values:
        logger.warning("Índice de saúde econômica não encontrado")
        
    return True

def create_test_data():
    """
    Cria dados de teste se não houver dados na camada silver.
    Isso permite testar a camada gold mesmo sem dados reais.
    """
    try:
        # Inicializa S3Handler
        s3_handler = S3Handler()
        
        # Verifica se já existem dados na camada silver
        silver_files = s3_handler.list_files(prefix="silver/economic_indicators")
        
        if silver_files:
            logger.info("Dados da camada silver já existem, não será necessário criar dados de teste")
            return True
            
        logger.info("Criando dados de teste para a camada silver...")
        
        # Cria dataframes de teste
        
        # IPCA - inflação mensal
        ipca_data = {
            'date': pd.date_range(start='2023-01-01', periods=12, freq='MS'),
            'value': [0.53, 0.84, 0.71, 0.61, 0.23, 0.16, -0.38, 0.23, 0.26, 0.24, 0.28, 0.56],
            'monthly_change_pct': [0.1, 0.31, -0.13, -0.1, -0.38, -0.07, -0.54, 0.61, 0.03, -0.02, 0.04, 0.28],
            'year_over_year_pct': [5.77, 5.6, 4.65, 4.18, 3.94, 3.16, 3.16, 3.43, 3.61, 4.82, 4.68, 4.62],
            'indicator': ['ipca'] * 12,
            'indicator_name': ['IPCA - Índice Nacional de Preços ao Consumidor Amplo'] * 12,
            'unit': ['%'] * 12,
            'frequency': ['monthly'] * 12
        }
        ipca_df = pd.DataFrame(ipca_data)
        
        # SELIC - taxa de juros
        selic_data = {
            'date': pd.date_range(start='2023-01-01', periods=12, freq='MS'),
            'value': [13.75, 13.75, 13.75, 13.75, 13.75, 13.75, 13.25, 13.25, 12.75, 12.25, 11.75, 11.25],
            'moving_avg_3m': [13.75, 13.75, 13.75, 13.75, 13.75, 13.75, 13.58, 13.42, 13.08, 12.75, 12.25, 11.75],
            'indicator': ['selic'] * 12,
            'indicator_name': ['Taxa SELIC'] * 12,
            'unit': ['%'] * 12,
            'frequency': ['monthly'] * 12
        }
        selic_df = pd.DataFrame(selic_data)
        
        # PIB - trimestral
        pib_data = {
            'date': pd.date_range(start='2023-01-01', periods=4, freq='QS'),
            'value': [2.4, 0.9, 0.1, 1.2],
            'quarterly_change_pct': [1.3, -1.5, -0.8, 1.1],
            'annual_change_pct': [4.0, 3.2, 2.1, 3.0],
            'indicator': ['pib'] * 4,
            'indicator_name': ['Produto Interno Bruto'] * 4,
            'unit': ['R$ bilhões'] * 4,
            'frequency': ['quarterly'] * 4
        }
        pib_df = pd.DataFrame(pib_data)
        
        # Câmbio - USD/BRL
        cambio_data = {
            'date': pd.date_range(start='2023-01-01', periods=12, freq='MS'),
            'value': [5.28, 5.17, 5.22, 5.05, 4.98, 4.85, 4.92, 5.03, 5.17, 5.23, 5.19, 5.05],
            'monthly_change_pct': [-1.2, -2.1, 0.9, -3.3, -1.4, -2.6, 1.4, 2.2, 2.8, 1.2, -0.8, -2.7],
            'volatility': [0.12, 0.09, 0.11, 0.08, 0.07, 0.09, 0.14, 0.12, 0.10, 0.08, 0.09, 0.11],
            'indicator': ['cambio'] * 12,
            'indicator_name': ['Taxa de Câmbio (USD/BRL)'] * 12,
            'unit': ['BRL'] * 12,
            'frequency': ['monthly'] * 12
        }
        cambio_df = pd.DataFrame(cambio_data)
        
        # Desemprego
        desemprego_data = {
            'date': pd.date_range(start='2023-01-01', periods=4, freq='QS'),
            'value': [8.8, 8.3, 7.9, 7.5],
            'quarterly_change_pp': [-0.4, -0.5, -0.4, -0.4],
            'annual_change_pp': [-2.1, -1.8, -1.3, -1.0],
            'indicator': ['desemprego'] * 4,
            'indicator_name': ['Taxa de Desemprego'] * 4,
            'unit': ['%'] * 4,
            'frequency': ['quarterly'] * 4
        }
        desemprego_df = pd.DataFrame(desemprego_data)
        
        # Salva dataframes na camada silver
        dfs = {
            'ipca': ipca_df,
            'selic': selic_df,
            'pib': pib_df,
            'cambio': cambio_df,
            'desemprego': desemprego_df
        }
        
        for name, df in dfs.items():
            # Gera caminho
            file_path = f"silver/economic_indicators/{name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
            
            # Salva no S3
            success = s3_handler.upload_dataframe(
                df=df,
                file_path=file_path.split('.')[0],  # Remove extensão que será adicionada pelo método
                layer='',  # Layer já está no path
                format='parquet'
            )
            
            if success:
                logger.info(f"Dados de teste para {name} salvos com sucesso")
            else:
                logger.error(f"Erro ao salvar dados de teste para {name}")
                return False
                
        return True
        
    except Exception as e:
        logger.error(f"❌ Erro ao criar dados de teste: {str(e)}")
        return False

if __name__ == "__main__":
    print("🔄 Iniciando testes de transformação Silver para Gold...\n")
    
    # Cria dados de teste se necessário
    print("Verificando disponibilidade de dados para teste...")
    data_available = create_test_data()
    
    if not data_available:
        print("⚠️ Não foi possível garantir dados para o teste")
    
    # Executa o teste
    print("\nProcessando camada Gold...")
    gold_result = test_gold_transformation()
    
    # Valida os dashboards
    if gold_result:
        print("\nValidando dashboards...")
        validation_result = validate_gold_dashboards()
    else:
        validation_result = False
        print("⚠️ Validação de dashboards ignorada devido a falhas anteriores")
    
    # Resumo final
    print("\n=== Resumo dos Testes ===")
    print(f"Transformação para Gold: {'✅ PASSOU' if gold_result else '❌ FALHOU'}")
    print(f"Validação de Dashboards: {'✅ PASSOU' if validation_result else '❌ FALHOU'}")
    
    # Resultado global
    if gold_result and validation_result:
        print("\n✅ TODOS OS TESTES PASSARAM COM SUCESSO!")
    else:
        print("\n❌ ALGUNS TESTES FALHARAM. Verifique os logs para mais detalhes.")