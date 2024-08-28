from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import nltk
import os
import uvicorn
from clean_news import get_raw_news_from_big_query, clean_news, predict_sentiment, write_clean_news_to_bq

# Ladda dina funktioner (se till att de finns i samma fil eller att de importeras korrekt)
# Om de är i en annan fil, använd: from <file_name> import <function_name>

# Säkerställ att NLTK-data laddas
nltk.download('vader_lexicon')

# Initialize FastAPI
app = FastAPI()

# Definiera en modell för inkommande data


class NewsRequest(BaseModel):
    project_id: str = 'tomastestproject-433206'
    dataset: str = 'testdb_1'
    table: str = 'raw_news'

# Definiera POST endpoint för att hämta, rensa och analysera nyheter


@app.post("/clean_news")
def clean_news_endpoint(request: NewsRequest):
    try:
        # Hämta data från BigQuery
        df = get_raw_news_from_big_query(
            request.table, request.project_id, request.dataset)

        # Rensa nyhetsdata
        cleaned_df = clean_news(df)

        # Gör sentimentanalyser
        predict_sentiment(cleaned_df)

        # Skriv de rensade nyheterna till BigQuery och få antalet rader som skrevs
        rows_written = write_clean_news_to_bq(cleaned_df)

        # Returnera resultat som JSON
        return {"message": "Data cleaned and written to BigQuery successfully.", "rows_written": rows_written}

    except Exception as e:
        # Hantera undantag och returnera felmeddelanden
        raise HTTPException(status_code=500, detail=str(e))


# Kör appen om detta script är huvudscripten

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    uvicorn.run(app, host="0.0.0.0", port=port)
