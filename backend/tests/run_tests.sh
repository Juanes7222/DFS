#!/bin/bash
# Script para ejecutar tests E2E del DFS - Adaptado para estructura backend/

set -e

echo "🧪 Ejecutando tests E2E del DFS..."

# Configuración
METADATA_URL="${DFS_METADATA_URL:-http://localhost:8000}"
PYTHON_CMD="${PYTHON_CMD:-python3}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
BACKEND_ROOT="$PROJECT_ROOT"

echo "Directorio del proyecto: $PROJECT_ROOT"
echo "Directorio backend: $BACKEND_ROOT"
echo "URL Metadata Service: $METADATA_URL"

# Configurar PYTHONPATH
export PYTHONPATH="$BACKEND_ROOT:$PYTHONPATH"

# Verificar que Python está disponible
if ! command -v $PYTHON_CMD &> /dev/null; then
    echo "Python no encontrado. Usa PYTHON_CMD para especificar el comando."
    exit 1
fi

# Verificar que podemos importar backend
if ! $PYTHON_CMD -c "import backend" &> /dev/null; then
    echo "No se puede importar el paquete backend"
    echo "PYTHONPATH: $PYTHONPATH"
    exit 1
fi

# Verificar que los servicios están corriendo
echo "🔍 Verificando servicios..."
if ! curl -s --retry 3 --retry-delay 2 "$METADATA_URL/api/v1/health" > /dev/null; then
    echo "Metadata Service no está disponible en $METADATA_URL"
    echo "   Inicia los servicios con: ./scripts/start_all.ps1"
    exit 1
fi

echo "Metadata Service está disponible"

# Configurar variables de entorno para tests
export DFS_METADATA_URL="$METADATA_URL"
export DFS_LOG_LEVEL="INFO"

# Ejecutar tests
echo "🚀 Ejecutando tests..."
cd "$SCRIPT_DIR"

$PYTHON_CMD -m pytest test_e2e.py -v \
    --tb=short \
    --color=yes \
    --durations=10 \
    --strict-markers \
    "$@"

EXIT_CODE=$?

if [ $EXIT_CODE -eq 0 ]; then
    echo "Todos los tests pasaron!"
else
    echo "Algunos tests fallaron"
fi

exit $EXIT_CODE