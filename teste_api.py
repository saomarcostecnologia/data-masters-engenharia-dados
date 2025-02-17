# test_bcb_api.py
import requests
import pandas as pd
from datetime import datetime, timedelta

def test_bcb_connection():
    """Testa a conexão com a API do BCB."""
    
    # Série do IPCA (433)
    url = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.433/dados"
    
    # Últimos 30 dias
    end_date = datetime.now()
    start_date = end_date - timedelta(days=60)
    
    params = {
        'formato': 'json',
        'dataInicial': start_date.strftime('%d/%m/%Y'),
        'dataFinal': end_date.strftime('%d/%m/%Y')
    }
    
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        
        df = pd.DataFrame(response.json())
        print("\nConexão bem sucedida!")
        print("\nÚltimos dados do IPCA:")
        print(df.tail())
        return True
        
    except Exception as e:
        print(f"\nErro na conexão: {str(e)}")
        return False

if __name__ == "__main__":
    test_bcb_connection()