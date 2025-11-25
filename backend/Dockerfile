FROM python:3.11-slim

WORKDIR /app

# Instalar dependencias
COPY backend/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar el backend directamente dentro del contenedor
COPY backend/ .

# Asegurarse de no incluir secretos
RUN rm -f .env || true
RUN rm -rf secrets || true
# RUN rm -rf backend/secrets || true
RUN rm -rf frontend

EXPOSE 8080

CMD ["python", "-m", "metadata.main"]