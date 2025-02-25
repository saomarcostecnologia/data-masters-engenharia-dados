# tests/integration/test_silver_transformation.py
import sys
import os
import logging
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path

# Adiciona o diretório raiz ao path para importar módulos do projeto
project_root = str(Path(__file__).parent.parent.parent)
sys.path.append(project_root)

from src.transformers.bronze_to_silver import EconomicIndicatorTransformer
from src.utils.helpers import (
    get_logger, 
    log_execution_time,
    log_dataframe_stats
)

# Configuração de logging
logger = get_logger("test_silver_transformation")

@log_execution_time(logger=logger, operation_name="Teste de Transformação Silver")
def test_silver_transformation():
    """Testa a transformação de dados da camada bronze para silver."""
    
    try:
        # Inicializa o transformador
        transformer = EconomicIndicatorTransformer()
        
        # Testa a conexão S3
        if not transformer.s3_handler.test_connection():
            logger.error("❌ Falha na conexão com o S3. Verifique suas credenciais.")
            return False
            
        logger.info("✅ Conexão com S3 estabelecida com sucesso!")
        
        # Processa cada indicador
        indicators = ['ipca', 'selic', 'pib', 'cambio', 'desemprego']
        results = {}
        
        for indicator in indicators:
            logger.info(f"\n📊 Processando indicador: {indicator}")
            success = transformer.process_indicator(indicator)
            results[indicator] = success
            
            if success:
                logger.info(f"✅ Transformação para {indicator} concluída com sucesso!")
            else:
                logger.error(f"❌ Falha na transformação para {indicator}.")
        
        # Resumo dos resultados
        logger.info("\n=== Resumo do Processamento ===")
        success_count = sum(1 for result in results.values() if result)
        total_count = len(indicators)
        
        success_rate = (success_count / total_count) * 100
        logger.info(f"Taxa de sucesso: {success_rate:.1f}% ({success_count}/{total_count})")
        
        for indicator, success in results.items():
            status = "✅ Sucesso" if success else "❌ Falha"
            logger.info(f"{indicator}: {status}")
            
        # Considera sucesso se pelo menos um indicador foi processado
        return success_count > 0
        
    except Exception as e:
        logger.error(f"❌ Erro durante o teste: {str(e)}")
        return False

def test_individual_transformation_functions():
    """Testa as funções de transformação individualmente com dados de exemplo."""
    
    try:
        # Inicializa o transformador
        transformer = EconomicIndicatorTransformer()
        
        # Cria dados de exemplo para testar transformações
        # IPCA - dados mensais simples
        ipca_data = {
            'data': pd.date_range(start='2023-01-01', periods=12, freq='MS'),
            'ipca': [0.5, 0.6, 0.7, 0.5, 0.4, 0.3, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7]
        }
        ipca_df = pd.DataFrame(ipca_data)
        
        # SELIC - dados diários
        selic_data = {
            'data': pd.date_range(start='2023-01-01', periods=120, freq='D'),
            'selic': [13.75] * 60 + [13.25] * 60  # Queda na metade do período
        }
        selic_df = pd.DataFrame(selic_data)
        
        # Testa transformações
        logger.info("\n=== Testando funções de transformação com dados sintéticos ===")
        
        # Testa IPCA
        logger.info("\nTestando transformação de IPCA...")
        ipca_silver = transformer.transform_ipca(ipca_df)
        log_dataframe_stats(ipca_silver, logger, "IPCA transformado")
        
        # Testa SELIC
        logger.info("\nTestando transformação de SELIC...")
        selic_silver = transformer.transform_selic(selic_df)
        log_dataframe_stats(selic_silver, logger, "SELIC transformado")
        
        logger.info("✅ Testes de transformação com dados sintéticos concluídos!")
        return True
        
    except Exception as e:
        logger.error(f"❌ Erro durante o teste de transformações: {str(e)}")
        return False

@log_execution_time(logger=logger, operation_name="Validação de Integridade dos Dados")
def validate_data_integrity():
    """Valida a integridade dos dados após transformação."""
    
    try:
        # Inicializa o transformador
        transformer = EconomicIndicatorTransformer()
        
        # Lista alguns arquivos transformados na camada silver para validação
        silver_files = transformer.s3_handler.list_files(prefix="silver/economic_indicators")
        
        if not silver_files:
            logger.warning("Nenhum arquivo encontrado na camada silver para validação")
            return False
            
        # Pega os arquivos mais recentes para cada indicador
        indicators = ['ipca', 'selic', 'pib', 'cambio', 'desemprego']
        latest_files = {}
        
        for indicator in indicators:
            indicator_files = [f for f in silver_files if indicator in f]
            if indicator_files:
                latest_files[indicator] = sorted(indicator_files)[-1]
        
        # Valida cada arquivo encontrado
        for indicator, file_path in latest_files.items():
            logger.info(f"Validando {indicator.upper()} (arquivo: {file_path})")
            
            # Carrega o arquivo
            df = transformer.s3_handler.download_file(file_path)
            
            if df is None or df.empty:
                logger.error(f"❌ Não foi possível carregar o arquivo {file_path}")
                continue
                
            # Verifica colunas essenciais
            essential_columns = ['date', 'value', 'indicator', 'unit', 'frequency']
            missing_columns = [col for col in essential_columns if col not in df.columns]
            
            if missing_columns:
                logger.error(f"❌ Colunas essenciais ausentes: {missing_columns}")
                continue
                
            # Verifica valores nulos em colunas principais
            null_counts = df[essential_columns].isnull().sum()
            if null_counts.sum() > 0:
                logger.warning(f"⚠️ Valores nulos encontrados: {null_counts.to_dict()}")
            
            # Validação específica por indicador
            if indicator == 'ipca':
                # Verificar se há colunas de variação
                if 'monthly_change_pct' not in df.columns:
                    logger.warning("⚠️ Coluna de variação mensal ausente no IPCA")
            
            # Mais validações específicas podem ser adicionadas aqui
            
            logger.info(f"✅ Validação de {indicator} concluída")
            
        return True
        
    except Exception as e:
        logger.error(f"❌ Erro durante validação de integridade: {str(e)}")
        return False

if __name__ == "__main__":
    print("🔄 Iniciando testes de transformação Bronze para Silver...\n")
    
    # Teste com dados sintéticos (não depende de dados no S3)
    synthetic_result = test_individual_transformation_functions()
    
    # Teste com dados reais do S3
    s3_result = test_silver_transformation()
    
    # Validação de integridade
    if s3_result:
        integrity_result = validate_data_integrity()
    else:
        integrity_result = False
        print("⚠️ Validação de integridade ignorada devido a falhas anteriores")
    
    # Resumo final
    print("\n=== Resumo dos Testes ===")
    print(f"Funções de transformação: {'✅ PASSOU' if synthetic_result else '❌ FALHOU'}")
    print(f"Transformação de dados reais: {'✅ PASSOU' if s3_result else '❌ FALHOU'}")
    print(f"Integridade dos dados: {'✅ PASSOU' if integrity_result else '❌ FALHOU'}")
    
    # Resultado global
    if synthetic_result and s3_result and integrity_result:
        print("\n✅ TODOS OS TESTES PASSARAM COM SUCESSO!")
    else:
        print("\n❌ ALGUNS TESTES FALHARAM. Verifique os logs para mais detalhes.")