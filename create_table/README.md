gcloud functions deploy create_bigquery_table \
  --runtime python39 \
  --trigger-http \
  --entry-point create_bigquery_table \
  --region us-central1


  gcloud builds submit --config /Users/tomasrydenstam/Desktop/Skola/test_project/create_table/cloudbuild.yaml

gcloud builds submit --config cloudbuild.yaml .



curl -X POST https://europe-west1-tomastestproject-433206.cloudfunctions.net/create-big-query-table \
     -H "Content-Type: application/json" \
     -d '{
           "table_name":"hej",
           "table_type": "clean_news_data"
         }'


gcloud functions deploy create-bigquery-table-2 \
  --runtime python39 \
  --trigger-http \
  --entry-point create_bigquery_table \
  --region europe-west1 \
  --allow-unauthenticated \
  --source=/Users/tomasrydenstam/Desktop/Skola/test_project/create_table \
  --service-account=service-accout-bigquery@tomastestproject-433206.iam.gserviceaccount.com
