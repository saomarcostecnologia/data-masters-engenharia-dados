# scripts/collect_economic_data.py
"""
Script para coletar dados econômicos de várias fontes.
Utiliza o padrão Factory para criar coletores e Command para executar operações.

Uso:
    python collect_economic_data.py --source bcb --indicators ipca,selic [--months 12]
    python collect_economic_data.py --source all --indicators all [--months 6]
"""

import os
import sys
import logging
import argparse
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional

# Adiciona o diretório raiz ao path para importar módulos do projeto
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Importa o factory e os coletores
from src.collectors.factory import CollectorFactory
from src.collectors.abstract_collector import AbstractCollector

# Configura logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("data_collection")

class CollectionCommand:
    """
    Implementa o padrão Command para encapsular uma operação de coleta.
    """
    
    def __init__(self, collector: AbstractCollector, indicators: List[str], 
                months: int = 12):
        """
        Inicializa o comando de coleta.
        
        Args:
            collector: Instância do coletor a ser usado
            indicators: Lista de indicadores a serem coletados
            months: Número de meses de dados a coletar
        """
        self.collector = collector
        self.indicators = indicators
        self.months = months
        
    def execute(self) -> Dict[str, bool]:
        """
        Executa a operação de coleta.
        
        Returns:
            Dicionário com status de cada indicador
        """
        # Calcula datas de início e fim
        end_date = datetime.now()
        start_date = end_date - timedelta(days=30.5 * self.months)
        
        # Executa coleta
        return self.collector.collect_and_store(
            indicators=self.indicators,
            start_date=start_date,
            end_date=end_date
        )

def list_available_indicators():
    """Lista todos os indicadores disponíveis agrupados por fonte."""
    collectors = CollectorFactory.get_all_collectors()
    
    logger.info("=== Indicadores Disponíveis ===")
    
    for source, collector in collectors.items():
        indicators = collector.get_available_indicators()
        logger.info(f"\nFonte: {source.upper()}")
        
        for indicator, config in indicators.items():
            name = config.get('name', indicator)
            frequency = config.get('frequency', 'N/A')
            logger.info(f"  - {indicator}: {name} ({frequency})")

def main():
    """Função principal."""
    parser = argparse.ArgumentParser(description='Coleta dados econômicos de várias fontes.')
    parser.add_argument('--source', type=str, default='all', 
                       help='Fonte de dados (bcb, ibge, ou all para todas)')
    parser.add_argument('--indicators', type=str, default='all',
                       help='Lista de indicadores separados por vírgula, ou "all" para todos')
    parser.add_argument('--months', type=int, default=12,
                       help='Número de meses de dados a coletar')
    parser.add_argument('--list', action='store_true',
                       help='Lista todos os indicadores disponíveis')
    
    args = parser.parse_args()
    
    # Lista indicadores se solicitado
    if args.list:
        list_available_indicators()
        return
    
    # Determina fontes a serem usadas
    sources = CollectorFactory.list_collectors() if args.source == 'all' else [args.source]
    
    # Processa cada fonte
    for source in sources:
        try:
            # Obtém o coletor
            collector = CollectorFactory.get_collector(source)
            
            if collector is None:
                logger.error(f"Fonte de dados não suportada: {source}")
                continue
                
            # Determina indicadores
            if args.indicators == 'all':
                indicators = list(collector.get_available_indicators().keys())
            else:
                indicators = [ind.strip() for ind in args.indicators.split(',')]
                
            # Cria e executa o comando de coleta
            command = CollectionCommand(collector, indicators, args.months)
            results = command.execute()
            
            # Exibe resultados
            logger.info(f"\n=== Resultados da Coleta: {source.upper()} ===")
            for indicator, success in results.items():
                status = "✅ Sucesso" if success else "❌ Falha"
                logger.info(f"{indicator}: {status}")
                
        except Exception as e:
            logger.error(f"Erro ao processar fonte {source}: {str(e)}")
    
    logger.info("\nProcesso de coleta concluído!")

if __name__ == "__main__":
    main()