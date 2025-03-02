#!/usr/bin/env python
"""
Script para executar os testes do projeto.

Uso:
    python run_tests.py [opções]

Opções:
    --unit       Executa apenas testes unitários
    --integration  Executa apenas testes de integração
    --all        Executa todos os testes (padrão)
    --cov        Gera relatório de cobertura
    --verbose    Mostra log detalhado
    --html       Gera relatório HTML
"""

import argparse
import subprocess
import sys
import os

def run_tests(unit=True, integration=True, coverage=False, verbose=False, html=False):
    """
    Executa os testes especificados.
    
    Args:
        unit: Se deve executar testes unitários
        integration: Se deve executar testes de integração
        coverage: Se deve gerar relatório de cobertura
        verbose: Se deve mostrar saída detalhada
        html: Se deve gerar relatório HTML
    
    Returns:
        int: Código de saída (0 = sucesso)
    """
    # Comando base
    cmd = ["pytest"]
    
    # Adiciona opções
    if verbose:
        cmd.append("-v")
    
    # Define caminhos de teste
    test_paths = []
    if unit:
        test_paths.append("tests/unit/")
    if integration:
        test_paths.append("tests/integration/")
    
    # Adiciona cobertura se solicitado
    if coverage:
        cmd.extend(["--cov=src", "--cov-report=term-missing"])
        if html:
            cmd.append("--cov-report=html")
    
    # Adiciona caminhos de teste
    cmd.extend(test_paths)
    
    # Executa testes
    print(f"Executando comando: {' '.join(cmd)}")
    return subprocess.call(cmd)

def main():
    """Função principal."""
    parser = argparse.ArgumentParser(description="Executa testes do projeto")
    parser.add_argument("--unit", action="store_true", help="Executar apenas testes unitários")
    parser.add_argument("--integration", action="store_true", help="Executar apenas testes de integração")
    parser.add_argument("--all", action="store_true", help="Executar todos os testes (padrão)")
    parser.add_argument("--cov", action="store_true", help="Gerar relatório de cobertura")
    parser.add_argument("--verbose", action="store_true", help="Mostrar saída detalhada")
    parser.add_argument("--html", action="store_true", help="Gerar relatório HTML")
    
    args = parser.parse_args()
    
    # Se nenhum tipo de teste foi especificado, executa todos
    if not (args.unit or args.integration):
        args.all = True
    
    # Define quais testes executar
    run_unit = args.unit or args.all
    run_integration = args.integration or args.all
    
    # Executa testes
    return run_tests(
        unit=run_unit, 
        integration=run_integration,
        coverage=args.cov,
        verbose=args.verbose,
        html=args.html
    )

if __name__ == "__main__":
    sys.exit(main())