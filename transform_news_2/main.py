from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import nltk
import os
import uvicorn
from typing import Optional
from clean_news import get_raw_news_from_big_query, clean_news, predict_sentiment, write_clean_news_to_bq, update_is_processed



# Säkerställ att NLTK-data laddas
nltk.download('vader_lexicon')

# Initialize FastAPI
app = FastAPI()

# Definiera en modell för inkommande data


class NewsRequest(BaseModel):
    project_id: Optional[str] = 'tomastestproject-433206'
    dataset: Optional[str] = 'testdb_1'
    fetch_table: Optional[str] = 'raw_news_with_uuid'
    write_table: Optional[str]= 'clean_news_copy'
    

# Definiera POST endpoint för att hämta, rensa och analysera nyheter


@app.post("/clean_news/")
def clean_news_endpoint(request: NewsRequest):
    try:
        # Hämta data från BigQuery
        df,ids = get_raw_news_from_big_query(
            request.fetch_table, request.project_id, request.dataset)
            
        # Rensa nyhetsdata
        cleaned_df = clean_news(df)

        # Gör sentimentanalyser
        predict_sentiment(df=cleaned_df)

        # Skriv de rensade nyheterna till BigQuery och få antalet rader som skrevs
        rows_written = write_clean_news_to_bq(data=cleaned_df,table=request.write_table)

        update_is_processed(id_string=ids, table=request.fetch_table)

        # Returnera resultat som JSON
        return {"message": "Data cleaned and written to BigQuery successfully.", "rows_written": rows_written}

    except Exception as e:
        # Hantera undantag och returnera felmeddelanden
        raise HTTPException(status_code=500, detail=str(e))


# Kör appen om detta script är huvudscripten

if __name__ == "__main__":
    #uvicorn.run(app, host="0.0.0.0", port=8000)
    pass
