# main.py
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Optional
from datetime import datetime, timedelta,timezone
from fetch_raw_data import fetch_news,save_raw_data_to_big_query, get_secret




app = FastAPI()

class QueryParameters(BaseModel):
    company: str
    from_date: Optional[str] = ((datetime.now(timezone.utc)) -timedelta(days=1)).strftime('%Y-%m-%d')
    to_date: Optional[str] = ((datetime.now(timezone.utc)) - timedelta(days=1)).strftime('%Y-%m-%d')
    table_name: Optional[str] = 'raw_news'


@app.post("/fetch-news/")
def fetch_news_and_save(params: QueryParameters):
      # Ersätt med din NewsAPI-nyckel

    try:
        # 1. Hämta nyheter från API:t
        news_data = fetch_news(
            company=params.company,
            from_date=params.from_date,
            to_date=params.to_date,
            api_key=get_secret('NEWS_API_KEY')
        )

        if not news_data or 'articles' not in news_data or len(news_data['articles']) == 0:
            raise HTTPException(status_code=404, detail="No data found.")

        # 2. Spara data till BigQuery
        save_raw_data_to_big_query(data=news_data, company=params.company,table=params.table_name)

        

        # 3. Returnera framgångsmeddelande med hämtade data
        return {"message": "Data fetched and saved successfully.", "Number of articels saved: ": news_data['totalResults'],"from_date":f"{params.from_date}","to_date":f"{params.to_date}","company":f"{params.company}"}

    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# Kör servern med uvicorn om du kör den lokalt
if __name__ == "__main__":
    # import uvicorn
    # uvicorn.run(app, host="0.0.0.0", port=8000)
    pass
