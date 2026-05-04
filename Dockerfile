# Use the official Python lightweight image
FROM python:3.10-slim

# Set the working directory
WORKDIR /app

# Install system dependencies required for psycopg2 (PostgreSQL adapter)
RUN apt-get update && apt-get install -y \
    libpq-dev gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first to leverage Docker cache
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code
COPY . .

# Set default command (can be overridden in docker-compose or run command)
# Example default: run the CSV pipeline
CMD ["python", "src/pipeline.py", "--source", "csv", "--input", "data/raw/customers.csv"]
