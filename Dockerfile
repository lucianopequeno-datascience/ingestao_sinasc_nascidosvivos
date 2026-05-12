FROM python:3.10-slim

# Define o diretório de trabalho no container
WORKDIR /app

# Instala dependências de sistema mínimas (se necessário para pacotes python)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copia as dependências e o script
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY extract_sinasc.py .

# Comando padrão de execução
CMD ["python", "extract_sinasc.py"]