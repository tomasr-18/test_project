gcloud scheduler jobs create http fetch-news-job \
  --schedule "0 23 * * *" \
  --http-method POST \
  --uri "https://workflowexecutions.googleapis.com/v1/projects/YOUR_PROJECT_ID/locations/YOUR_REGION/workflows/YOUR_WORKFLOW_NAME/executions" \
  --oauth-service-account-email YOUR_SERVICE_ACCOUNT_EMAIL \
  --time-zone "UTC"


Ersätt:

YOUR_PROJECT_ID med ditt Google Cloud-projekt-ID.
YOUR_REGION med den region där ditt workflow körs.
YOUR_WORKFLOW_NAME med namnet på ditt workflow.
YOUR_SERVICE_ACCOUNT_EMAIL med e-postadressen till din servicekonto som har rätt behörigheter att anropa Workflows.