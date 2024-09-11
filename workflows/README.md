## GÖR SÅ ATT WORKFLOW KOMMER ÅT SERVICES MED AUTHENTICATION ##
## BYT UT "fetch-raw-news-app" MOT EGENT NAMN ##

##
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
##

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


    ---


gcloud workflows deploy transfer-meta_data \
    --source=/Users/tomasrydenstam/Desktop/Skola/test_project/workflows/update_news_meta_data.yaml\
    --location=europe-west1 \
    --service-account=workflow-service-account@tomastestproject-433206.iam.gserviceaccount.com


    gcloud workflows run transfer-meta_data --location=europe-west1 --project=tomastestproject-433206

    --

    gcloud workflows deploy backfill_news \
    --source=/Users/tomasrydenstam/Desktop/Skola/test_project/workflows/backfill_news.yaml\
    --location=europe-west1 \
    --service-account=workflow-service-account@tomastestproject-433206.iam.gserviceaccount.com


    --

      gcloud workflows deploy create_table_in_big_query \
    --source=/Users/tomasrydenstam/Desktop/Skola/test_project/workflows/create_table.yaml\
    --location=europe-west1 \
    --service-account=workflow-service-account@tomastestproject-433206.iam.gserviceaccount.com
    
    --
      gcloud workflows deploy train_and_predict_model \
    --source=/Users/tomasrydenstam/Desktop/Skola/test_project/workflows/train_and_predict.yaml\
    --location=europe-west1 \
    --service-account=workflow-service-account@tomastestproject-433206.iam.gserviceaccount.com

    --

      gcloud workflows deploy transfer_true_values_to_predictions \
    --source=/Users/tomasrydenstam/Desktop/Skola/test_project/workflows/transfer_true_values_to_predictions.yaml\
    --location=europe-west1 \
    --service-account=workflow-service-account@tomastestproject-433206.iam.gserviceaccount.com  