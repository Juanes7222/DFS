#!/bin/bash
# Script para ejecutar tests E2E

set -e

echo "🧪 Ejecutando tests E2E del DFS..."

# Verificar que los servicios están corriendo
if ! curl -s http://localhost:8000/api/v1/health > /dev/null; then
    echo "❌ Metadata Service no está disponible en http://localhost:8000"
    echo "   Inicia los servicios con: docker-compose up -d"
    exit 1
fi

# Instalar dependencias de tests
pip3 install -r requirements.txt

# Ejecutar tests
pytest test_e2e.py -v --tb=short

echo "✅ Tests completados"
