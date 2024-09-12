import datetime
from flask import Flask, render_template, request
import plotly.express as px
import plotly.graph_objs as go
import json
import plotly
from google.cloud import secretmanager, bigquery


def get_secret(secret_name='bigquery-accout-secret') -> str:
    """Fetches a secret from Google Cloud Secret Manager.

    Args:
        secret_name (str): The name of the secret in Secret Manager.

    Returns:
        str: The secret data as a string.
    """
    # Instansiera en klient för Secret Manager
    client = secretmanager.SecretManagerServiceClient()

    # Bygg sökvägen till den hemlighet du vill hämta
    project_id = 'tomastestproject-433206'  # Ersätt med ditt projekt-ID
    secret_path = f"projects/{project_id}/secrets/{secret_name}/versions/latest"

    # Hämta den senaste versionen av hemligheten
    response = client.access_secret_version(name=secret_path)

    # Dekoda hemligheten till en sträng
    secret_data = response.payload.data.decode('UTF-8')

    return secret_data

def get_data_from_bigquery():

    secret_data = get_secret()

    # Ladda JSON-strängen till en dictionary
    service_account_info = json.loads(secret_data)

    # Initiera BigQuery-klienten med service account
    client = bigquery.Client.from_service_account_info(service_account_info)

    # Build your SQL query
    query = f"""
    WITH ranked_predictions AS (
        SELECT 
            company,
            model_name,
            true_value,
            predicted_value,
            date,
            -- Mean Absolute Percentage Error (MAPE)
            CASE 
                WHEN true_value != 0 THEN ABS((true_value - predicted_value) / true_value) * 100
                ELSE NULL  -- Handle division by zero
            END AS mape,
            -- Mean Absolute Error (MAE)
            ABS(true_value - predicted_value) AS mae,
            ROW_NUMBER() OVER (PARTITION BY model_name ORDER BY date DESC) AS rn
        FROM 
            `tomastestproject-433206.testdb_1.predictions`
        WHERE
            company IS NOT NULL AND
            model_name IS NOT NULL AND
            true_value IS NOT NULL AND
            predicted_value IS NOT NULL AND
            date IS NOT NULL
    )
    SELECT 
        company,
        model_name,
        true_value,
        predicted_value,
        date,
        ROUND(mape, 3) AS mape,
        ROUND(mae, 3) as mae
    FROM 
        ranked_predictions
    WHERE 
        rn = 1;
        """

    # Execute the SQL query
    query_job = client.query(query)

    # Fetch the results
    results = query_job.result()

    # Convert results to a DataFrame
    df = results.to_dataframe().sort_values('date', ascending=False)

    # Check if DataFrame is empty and raise an error if needed
    if df.empty:
        raise ValueError("No unprocessed data found")


    return df




# Initialize Flask app
app = Flask(__name__)

@app.route('/', methods=['GET', 'POST'])
def dashboard():

    
    if request.method == 'POST':
        selected_option = request.form.get('dropdown', 'AAPL')  # Get the selected option from the form
    else:
        selected_option = 'AAPL'  # Default selected option
    
    df = get_data_from_bigquery()
    models = df.to_dict(orient='records')

    # Filter the data to only include the selected company
    filtered_df = df[df['company'] == selected_option]

    # Sort the data by date to get the latest model
    filtered_df = filtered_df.sort_values(by='date', ascending=False)

        # Extract details of the latest model
    if not filtered_df.empty:
        latest_model_name = filtered_df.iloc[0]['model_name']
        latest_date = filtered_df.iloc[0]['date'].date()
        latest_prediction = round(filtered_df.iloc[0]['predicted_value'], 2)
        latest_true_value = round(filtered_df.iloc[0]['true_value'], 2)
        difference_price = round(latest_true_value - latest_prediction, 2)
    else:
        # Handle case where there is no data for the selected company
        latest_model_name = "No data"
        latest_date = None
        latest_prediction = 0
        latest_true_value = 0
        difference_price = 0



    # Example data for the graph
    data = [
        go.Scatter(
            x=[1, 2, 3, 4],
            y=[10, 11, 12, 13],
            mode='lines+markers',
            name='Example'
        )
    ]
    # Convert the figure to JSON
    graphJSON = json.dumps(data, cls=plotly.utils.PlotlyJSONEncoder)



    return render_template('index.html', 
                           selected_option=selected_option,
                           latest_model_name=latest_model_name, 
                           latest_date=latest_date, 
                           difference_price=difference_price, 
                           prediction=latest_prediction, 
                           true_value=latest_true_value, 
                           models=models, 
                           graphJSON=graphJSON
                           )

if __name__ == '__main__':
    app.run(debug=True)