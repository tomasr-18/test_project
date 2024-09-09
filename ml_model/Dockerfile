# Använd en officiell Python-bild som bas
FROM python:3.9-slim

# Ställ in arbetskatalogen i containern
WORKDIR /app

# Kopiera requirements.txt till arbetskatalogen
COPY requirements.txt .

ENV SECRET_NAME_ENV="my-env-file"
ENV SECRET_NAME_SA="bigquery-accout-secret"
ENV PROJECT_ID="tomastestproject-433206"

# Installera Python-biblioteken som behövs
RUN pip install --no-cache-dir -r requirements.txt

# Kopiera all kod till containern
COPY . .

# Exponera port 8080
EXPOSE 8080

# Kör load_env.py för att ladda miljövariabler
# Kör både load_env.py och starta appen när containern startas
#CMD ["bash", "-c", "python load_env.py && uvicorn main:app --host 0.0.0.0 --port 8080"]

# Starta Flask-applikationen
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080"]
