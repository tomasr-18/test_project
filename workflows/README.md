kör följande steg i terminalen för fetch_news

gcloud run services add-iam-policy-binding fetch-raw-news-app \
    --member="serviceAccount:service-accout-bigquery@tomastestproject-433206.iam.gserviceaccount.com" \
    --role="roles/run.invoker" \
    --region=europe-west1 \
    --project=tomastestproject-433206


gcloud workflows deploy fetch_news \
    --source=/Users/tomasrydenstam/Desktop/Skola/test_project/workflows/fetch_news.yaml\
    --location=europe-west1 \
    --service-account=workflow-service-account@tomastestproject-433206.iam.gserviceaccount.com

gcloud workflows run fetch_news --location=europe-west1 --project=tomastestproject-433206

----


gcloud workflows deploy fetch_news_ny \
    --source=/Users/tomasrydenstam/Desktop/Skola/test_project/workflows/fetch_news_ny.yaml\
    --location=europe-west1 \
    --service-account=workflow-service-account@tomastestproject-433206.iam.gserviceaccount.com


    gcloud workflows run fetch_news_ny --location=europe-west1 --project=tomastestproject-433206

-----

gcloud run services add-iam-policy-binding clean-news-app-ny \
    --member="serviceAccount:service-accout-bigquery@tomastestproject-433206.iam.gserviceaccount.com" \
    --role="roles/run.invoker" \
    --region=europe-west1 \
    --project=tomastestproject-433206

gcloud workflows deploy clean_news_ny \
    --source=/Users/tomasrydenstam/Desktop/Skola/test_project/workflows/clean_news_ny.yaml\
    --location=europe-west1 \
    --service-account=workflow-service-account@tomastestproject-433206.iam.gserviceaccount.com


    gcloud workflows run clean_news_ny --location=europe-west1 --project=tomastestproject-433206