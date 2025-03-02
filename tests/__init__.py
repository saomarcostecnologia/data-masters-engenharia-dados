"""Inicialização do pacote de testes.

Este arquivo configura o PYTHONPATH para incluir o diretório raiz do projeto,
permitindo que os testes importem módulos do pacote 'src' sem problemas.
"""

import sys
import os

# Adiciona o diretório raiz do projeto ao path para permitir importações da pasta 'src'
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)