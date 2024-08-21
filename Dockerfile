# Använd en Python-bild
FROM python:3.9-slim

# Sätt arbetskatalogen i containern
WORKDIR /app

# Kopiera kravfilen och installera beroenden
COPY req.txt req.txt
RUN pip install -r req.txt

# Kopiera resten av applikationen
COPY . .

# Exponera porten som Flask använder
EXPOSE 8080

# Starta Flask-applikationen
CMD ["python", "app.py"]
