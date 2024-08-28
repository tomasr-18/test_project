# Använd en officiell Python-bild som bas
FROM python:3.9-slim

# Ställ in arbetskatalogen i containern
WORKDIR /fetch_news

# Kopiera requirements.txt till arbetskatalogen
COPY req.txt .

# Installera Python-biblioteken som behövs
RUN pip install --no-cache-dir -r req.txt

# Kopiera all kod till containern
COPY . .


EXPOSE 8080

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080"]
