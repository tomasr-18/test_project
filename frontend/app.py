from flask import Flask, render_template, request
import plotly.graph_objs as go
import json
import pandas as pd
import plotly
from google.cloud import secretmanager, bigquery


def get_secret(secret_name="bigquery-accout-secret") -> str:
    """Fetches a secret from Google Cloud Secret Manager.

    Args:
        secret_name (str): The name of the secret in Secret Manager.

    Returns:
        str: The secret data as a string.
    """
    # Insantiate a Secret Manager client
    client = secretmanager.SecretManagerServiceClient()

    # Set the secret path
    project_id = "tomastestproject-433206" 
    secret_path = f"projects/{project_id}/secrets/{secret_name}/versions/latest"

    # Access the secret version
    response = client.access_secret_version(name=secret_path)

    # decode the secret data
    secret_data = response.payload.data.decode("UTF-8")

    return secret_data


def get_data_from_bigquery() -> pd.DataFrame:
    secret_data = get_secret()

    # Load the json data into a dictionary
    service_account_info = json.loads(secret_data)

    # Initialize a BigQuery client
    client = bigquery.Client.from_service_account_info(service_account_info)

    # Query to fetch the latest predictions and stock data
    query = """
WITH ranked_predictions AS (
    SELECT 
        company,
        model_name,
        true_value,
        predicted_value,
        DATE(date) AS date,  -- Ensure TIMESTAMP is cast to DATE
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
),
latest_asd_per_rp AS (
    SELECT
        rp.*,
        asd.avg_score_title,
        asd.pub_date,
        asd.close,
        ROW_NUMBER() OVER (
            PARTITION BY rp.company, rp.date
            ORDER BY asd.pub_date DESC
        ) AS rn_asd
    FROM
        ranked_predictions rp
    LEFT JOIN
        `tomastestproject-433206.testdb_1.avg_scores_and_stock_data_right` asd
    ON
        rp.company = asd.company
        AND asd.pub_date <= DATE_SUB(rp.date, INTERVAL 1 DAY)
    WHERE
        rp.rn = 1
)
SELECT
    avg_score_title,
    company,
    model_name,
    true_value,
    predicted_value,
    date,
    ROUND(mape, 3) AS mape,
    ROUND(mae, 3) AS mae,
    pub_date,
    close
FROM
    latest_asd_per_rp
WHERE
    rn_asd = 1;

        """

    # Execute the SQL query
    query_job = client.query(query)

    # Fetch the results
    results = query_job.result()

    # Convert results to a DataFrame
    df = results.to_dataframe().sort_values("date", ascending=False)

    # Check if DataFrame is empty and raise an error if needed
    if df.empty:
        raise ValueError("No unprocessed data found")

    return df


# Initialize Flask app
app = Flask(__name__)


@app.route("/", methods=["GET", "POST"])
def dashboard():
    if request.method == "POST":
        selected_option = request.form.get(
            "dropdown", "AAPL"
        )  # Get the selected option from the form
    else:
        selected_option = "AAPL"  # Default selected option

    df = get_data_from_bigquery()

    # Filter the data to only include the selected company
    filtered_df = df[df["company"] == selected_option]
    models = filtered_df.to_dict(orient="records")

    # Sort the data by date to get the latest model
    filtered_df = filtered_df.sort_values(by="date", ascending=False)

    # Extract details of the latest model
    if not filtered_df.empty:
        latest_model_name = filtered_df.iloc[0]["model_name"]
        latest_date = filtered_df.iloc[0]["date"]
        latest_prediction = round(filtered_df.iloc[0]["predicted_value"], 3)
        latest_true_value = round(filtered_df.iloc[0]["true_value"], 3)
        difference_price = round(latest_true_value - latest_prediction, 3)
        closing_price = round(filtered_df.iloc[0]["close"], 3)
    else:
        # Handle case where there is no data for the selected company
        latest_model_name = "No data"
        latest_date = None
        latest_prediction = 0
        latest_true_value = 0
        difference_price = 0

    if latest_prediction > closing_price and latest_true_value > closing_price:
        prediction_value = "Correct"
    elif latest_prediction < closing_price and latest_true_value < closing_price:
        prediction_value = "Correct"
    else:
        prediction_value = "Incorrect"

    if latest_prediction > closing_price:
        prediction_direction = "🚀"
    elif latest_prediction < closing_price:
        prediction_direction = "📉"

    # Extract data from filtered_df
    dates = filtered_df["date"]
    predicted_values = filtered_df["predicted_value"]
    true_values = filtered_df["true_value"]
    sentiment_scores = filtered_df["avg_score_title"]

    # Create the graph data
    data_stocks = [
        go.Scatter(
            x=dates, y=predicted_values, mode="lines+markers", name="Predicted Value"
        ),
        go.Scatter(x=dates, y=true_values, mode="lines+markers", name="True Value"),
    ]
    # Create the graph data
    data_sentiment = [
        go.Scatter(
            x=dates, y=sentiment_scores, mode="lines+markers", name="Predicted Value"
        )
    ]

    max_abs_sentiment = max(abs(filtered_df["avg_score_title"].min()), abs(filtered_df["avg_score_title"].max()))
    y_range = [-max_abs_sentiment-0.1, max_abs_sentiment+0.1]

    # Create the layout with the company name as the title
    layout_stocks = go.Layout(
        title=f"{selected_option} Stock Prices",
        xaxis=dict(title="Date"),
        yaxis=dict(title="Price (USD)"),
    )
    # Create the layout with the company name as the title
    layout_sentiment = go.Layout(
        title=f"{selected_option} Sentiment Analysis",
        xaxis=dict(title="Date"),
        yaxis=dict(title="Sentmiemt Score",
                   zeroline=True,
                    zerolinecolor='gray',
                    zerolinewidth=1,
                    range=y_range),
                    
    )

    # Create the figure with data and layout
    fig = go.Figure(data=data_stocks, layout=layout_stocks)
    # Create the figure with data and layout
    fig1 = go.Figure(data=data_sentiment, layout=layout_sentiment)

    # Convert the figure to JSON
    graphJSON1 = json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)

    graphJSON2 = json.dumps(fig1, cls=plotly.utils.PlotlyJSONEncoder)

    return render_template(
        "index.html",
        closing_price=closing_price,
        prediction_direction=prediction_direction,
        prediction_value=prediction_value,
        selected_option=selected_option,
        latest_model_name=latest_model_name,
        latest_date=latest_date,
        difference_price=difference_price,
        prediction=latest_prediction,
        true_value=latest_true_value,
        models=models,
        graphJSON1=graphJSON1,
        graphJSON2=graphJSON2,
    )


if __name__ == "__main__":
    app.run(debug=True)
