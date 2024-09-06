import os
from dotenv import load_dotenv
from google.cloud import storage
#from google.cloud import bigquery
import pandas as pd
from sklearn.linear_model import SGDRegressor
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import make_pipeline
import joblib


# Load .env file
load_dotenv(dotenv_path='.env')

# Access the environment variable
credentials_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')

# Initialize Google Cloud Storage client
storage_client = storage.Client()

# Access the Google Cloud Storage bucket
bucket_name = 'machine-models'
bucket = storage_client.get_bucket(bucket_name)


def display_bucket_contents():
    # List all blobs in the bucket
    blobs = bucket.list_blobs()

    for blob in blobs:
        print(blob.name)



def load_model():
    model_name = 'stock_model.pkl'
    model_file = f'{model_name}'

    try:
        blob = bucket.blob(model_name)
        blob.download_to_filename(model_file)
        with open(model_file, 'rb') as f:
            model = joblib.load(f)
        print(f"Model {model_name} loaded successfully.")
    except Exception:
        print(f"Model {model_name} not found. Initializing a new model.")
        model = make_pipeline(StandardScaler(), SGDRegressor())
    return model

def save_model(model,model_name):
    model_name = 'stock_model_2.pkl'
    model_file = f'{model_name}'
    with open(model_file, 'wb') as f:
        joblib.dump(model, f)
    blob = bucket.blob(model_name)
    blob.upload_from_filename(model_file)
    print(f"Model {model_name} saved successfully.")

# Define the model training function
def train_model(df:pd.DataFrame,):
    data = pd.read_csv('./data/data.csv') 
    X = data[['Open', 'High', 'Low', 'Volume']]  # Replace with actual feature columns
    y = data['Close']  # Replace with actual target column

    model = load_model()

    # Train the model incrementally
    model.named_steps['sgdregressor'].partial_fit(X, y)

    # Save the updated model
    save_model(model)

    # Evaluate the model
    predictions = model.predict(X)
    mae = ((y - predictions).abs()).mean()
    print(f'MAE: {mae}')


def upload_to_bucket(model_path: str):
    # Specify the local file path
    local_file_path: str = model_path

    # Specify the destination blob name in the bucket
    destination_blob_name: str = f'images/{local_file_path}'

    # Upload the file to the bucket
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(local_file_path)

    print(f"File '{local_file_path}' uploaded to '{destination_blob_name}' in the bucket.")


if __name__ == '__main__':

    display_bucket_contents()
    train_model()
    #pickled_model = model_training()

    #upload_to_bucket(pickled_model)