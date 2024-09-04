import os
import pickle
from river import linear_model, preprocessing, compose
from river.feature_extraction import Lagger
from google.cloud import bigquery, storage
from datetime import datetime

# Initialize BigQuery client
client = bigquery.Client()

def fetch_data_from_bigquery():
    query = """
    SELECT * FROM `your_project.your_dataset.your_table`
    """
    query_job = client.query(query)
    results = query_job.result()
    
    # Convert results to a suitable format for your model
    data_stream = []
    for row in results:
        x = {'feature1': row['feature1'], 'feature2': row['feature2']}  # Adjust based on your schema
        y = row['target']
        data_stream.append((x, y))
    
    return data_stream

def save_model(model):
    # Generate a timestamped filename
    timestamp = datetime.now().strftime('%Y%m%d')
    filename = f'model_{timestamp}.pkl'
    
    # Save the model to a local file
    with open(filename, 'wb') as f:
        pickle.dump(model, f)
    
    # Upload the model file to Google Cloud Storage
    storage_client = storage.Client()
    bucket = storage_client.bucket('your-bucket-name')
    blob = bucket.blob(filename)
    blob.upload_from_filename(filename)

def load_model():
    storage_client = storage.Client()
    bucket = storage_client.bucket('your-bucket-name')
    
    # List all blobs in the bucket
    blobs = list(bucket.list_blobs())
    
    if blobs:
        # Find the blob with the most recent timestamp
        latest_blob = max(blobs, key=lambda b: b.name)
        
        # Download the latest model file from Google Cloud Storage
        latest_blob.download_to_filename('latest_model.pkl')
        with open('latest_model.pkl', 'rb') as f:
            model = pickle.load(f)
    else:
        model = (
            Lagger(n_lags=5) |
            preprocessing.StandardScaler() |
            linear_model.LinearRegression(intercept_lr=0.1)
        )
    return model

def train_model(data_stream):
    model = load_model()
    for x, y in data_stream:
        y_pred = model.predict_one(x)
        model.learn_one(x, y)
    return model

if __name__ == "__main__":
    # Fetch data from BigQuery
    data_stream = fetch_data_from_bigquery()
    # Train the model
    trained_model = train_model(data_stream)
    # Save the trained model
    save_model(trained_model)