"""Testes de integração para APIs externas utilizadas pelo projeto."""

import pytest
import requests
import pandas as pd
from datetime import datetime, timedelta

def test_bcb_api_connection():
    """Testa a conexão com a API do Banco Central do Brasil."""
    
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
        
        # Verificações
        assert df is not None
        assert len(df) > 0
        assert 'data' in df.columns
        assert 'valor' in df.columns
        
        print("\nConexão com BCB bem sucedida!")
        print("\nÚltimos dados do IPCA:")
        print(df.tail())
        
        return True
        
    except Exception as e:
        print(f"\nErro na conexão com BCB: {str(e)}")
        pytest.fail(f"Falha na conexão com a API do BCB: {str(e)}")

def test_ibge_api_connection():
    """Testa a conexão com a API do IBGE."""
    
    # URL da API SIDRA para o IPCA-15
    url = "https://servicodados.ibge.gov.br/api/v3/agregados/7062/periodos/201901-202001/variaveis/all"
    
    try:
        response = requests.get(url)
        response.raise_for_status()
        
        data = response.json()
        
        # Verificações
        assert data is not None
        assert len(data) > 0
        assert 'resultados' in data[0]
        
        print("\nConexão com IBGE bem sucedida!")
        return True
        
    except Exception as e:
        print(f"\nErro na conexão com IBGE: {str(e)}")
        pytest.fail(f"Falha na conexão com a API do IBGE: {str(e)}")

if __name__ == "__main__":
    try:
        test_bcb_api_connection()
        test_ibge_api_connection()
        print("\nTodos os testes de API passaram com sucesso!")
    except Exception as e:
        print(f"\nErro nos testes de API: {str(e)}")