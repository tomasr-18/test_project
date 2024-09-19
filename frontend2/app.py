from flask import Flask, render_template, request, abort
import plotly.graph_objs as go
import json
import pandas as pd
import plotly
from google.cloud import secretmanager, bigquery


def get_secret(secret_name="bigquery-accout-secret") -> str:
    """Fetches a secret from Google Cloud Secret Manager."""
    client = secretmanager.SecretManagerServiceClient()
    project_id = "tomastestproject-433206"
    secret_path = f"projects/{project_id}/secrets/{secret_name}/versions/latest"
    response = client.access_secret_version(name=secret_path)
    secret_data = response.payload.data.decode("UTF-8")
    return secret_data


def get_api_key() -> str:
    """Fetches the API key from Google Cloud Secret Manager."""
    return get_secret(secret_name="api-key-secret")


def get_data_from_bigquery() -> pd.DataFrame:
    secret_data = get_secret()
    service_account_info = json.loads(secret_data)
    client = bigquery.Client.from_service_account_info(service_account_info)

    query = """
    -- Your BigQuery SQL query here
    """
    query_job = client.query(query)
    results = query_job.result()
    df = results.to_dataframe().sort_values("date", ascending=False)

    if df.empty:
        raise ValueError("No unprocessed data found")
    return df


# Initialize Flask app
app = Flask(__name__)

# Route decorator for checking API key


def require_api_key(func):
    """Decorator to enforce API key authentication."""
    def wrapper(*args, **kwargs):
        # Fetch API key from headers or query params
        api_key = request.headers.get(
            'x-api-key') or request.args.get('api_key')
        valid_api_key = get_api_key()

        if not api_key:
            print("API key not provided")  # Log when API key is missing
            abort(403, description="API key is missing")

        if api_key != valid_api_key:
            # Log when invalid API key is provided
            print(f"Invalid API key: {api_key}")
            abort(403, description="Invalid API key")

        return func(*args, **kwargs)
    return wrapper


@app.route("/", methods=["GET", "POST"])
@require_api_key  # Apply the API key check to this route
def dashboard():
    if request.method == "POST":
        selected_option = request.form.get("dropdown", "AAPL")
    else:
        selected_option = "AAPL"

    df = get_data_from_bigquery()
    filtered_df = df[df["company"] == selected_option]
    models = filtered_df.to_dict(orient="records")

    filtered_df = filtered_df.sort_values(by="date", ascending=False)

    if not filtered_df.empty:
        latest_model_name = filtered_df.iloc[0]["model_name"]
        latest_date = filtered_df.iloc[0]["date"]
        latest_prediction = round(filtered_df.iloc[0]["predicted_value"], 3)
        latest_true_value = round(filtered_df.iloc[0]["true_value"], 3)
        difference_price = round(latest_true_value - latest_prediction, 3)
        closing_price = round(filtered_df.iloc[0]["close"], 3)
    else:
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

    dates = filtered_df["date"]
    predicted_values = filtered_df["predicted_value"]
    true_values = filtered_df["true_value"]
    sentiment_scores = filtered_df["avg_score_title"]

    data_stocks = [
        go.Scatter(
            x=dates, y=predicted_values, mode="lines+markers", name="Predicted Value"
        ),
        go.Scatter(x=dates, y=true_values,
                   mode="lines+markers", name="True Value"),
    ]

    data_sentiment = [
        go.Scatter(
            x=dates, y=sentiment_scores, mode="lines+markers", name="Sentiment Score"
        )
    ]

    max_abs_sentiment = max(abs(filtered_df["avg_score_title"].min()), abs(
        filtered_df["avg_score_title"].max()))
    y_range = [-max_abs_sentiment-0.1, max_abs_sentiment+0.1]

    layout_stocks = go.Layout(
        title=f"{selected_option} Stock Prices",
        xaxis=dict(title="Date"),
        yaxis=dict(title="Price (USD)"),
    )

    layout_sentiment = go.Layout(
        title=f"{selected_option} Sentiment Analysis",
        xaxis=dict(title="Date"),
        yaxis=dict(title="Sentiment Score",
                   zeroline=True,
                   zerolinecolor='gray',
                   zerolinewidth=1,
                   range=y_range),
    )

    fig = go.Figure(data=data_stocks, layout=layout_stocks)
    fig1 = go.Figure(data=data_sentiment, layout=layout_sentiment)

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
