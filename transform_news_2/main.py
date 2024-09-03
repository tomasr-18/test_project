from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import nltk
from typing import Optional
from clean_news import get_raw_news_from_big_query, clean_news, predict_sentiment, write_clean_news_to_bq, update_is_processed,transfer_ids_to_meta_data
import logging



# Säkerställ att NLTK-data laddas
nltk.download('vader_lexicon')

# Initialize FastAPI
app = FastAPI()

# Definiera en modell för inkommande data


class NewsRequest(BaseModel):
    project_id: Optional[str] = 'tomastestproject-433206'
    dataset: Optional[str] = 'testdb_1'
    fetch_table: Optional[str] = 'raw_news_data'
    write_table: Optional[str]= 'clean_news_data'
    meta_data_table: Optional[str] = 'raw_news_meta_data'
    
class TransferData(BaseModel):
    project_id: Optional[str] = 'tomastestproject-433206'
    dataset: Optional[str] = 'testdb_1'
    table_from: Optional[str] = 'raw_news_data'
    table_to: Optional[str] = 'raw_news_meta_data'


# Definiera POST endpoint för att hämta, rensa och analysera nyheter


@app.post("/clean_news/")
def clean_news_endpoint(request: NewsRequest):
    try:
        # Hämta data från BigQuery
        df,ids = get_raw_news_from_big_query(
                                            raw_data_table=request.fetch_table, 
                                            meta_data_table=request.meta_data_table, 
                                            project_id=request.project_id, 
                                            dataset=request.dataset
                                            )
        
        if df is None or ids is None or df.empty:
            return {"message": "There is no unprocessed data to fetch."}
            
        # Rensa nyhetsdata
        cleaned_df = clean_news(df=df)

        # Gör sentimentanalyser
        predict_sentiment(df=cleaned_df)

        # Skriv de rensade nyheterna till BigQuery och få antalet rader som skrevs
        rows_written = write_clean_news_to_bq(data=cleaned_df,table=request.write_table)

        update_is_processed(id_string=ids, table=request.meta_data_table)

        # Returnera resultat som JSON
        return {"message": "Data cleaned and written to BigQuery successfully.", "rows_written": rows_written}

    except Exception as e:
            # Logga detaljer om felet och returnera ett HTTP-fel med detaljer
            logging.error(f"Error occurred: {e}")
            raise HTTPException(status_code=500, detail=f"Error processing request: {str(e)}")

@app.post("/transfer_to_meta_data/")
def transfer_to_meta_data_endpoint(request:TransferData):
    try:
        rows_inserted = transfer_ids_to_meta_data(
                                table_from=request.table_from,
                                table_to=request.table_to,
                                project_id=request.project_id,
                                dataset=request.dataset
                                )
        return {"messege": f"{rows_inserted} rows inserted to {request.table_to}"}
    except Exception as e:
            # Logga detaljer om felet och returnera ett HTTP-fel med detaljer
            logging.error(f"Error occurred: {e}")
            raise HTTPException(status_code=500, detail=f"Error processing request: {str(e)}")
        

# Kör appen om detta script är huvudscripten

if __name__ == "__main__":
    #uvicorn.run(app, host="0.0.0.0", port=8000)
    pass
