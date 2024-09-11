import pytest
from unittest.mock import patch, MagicMock
import pandas as pd
from clean_news import (
    get_raw_news_from_big_query,
    update_is_processed,
    clean_news,
    predict_sentiment,
    write_clean_news_to_bq,
    transfer_ids_to_meta_data
)

# Test for get_raw_news_from_big_query


@patch('clean_news.bigquery.Client')
def test_get_raw_news_from_big_query(mock_bigquery_client):
    mock_client = MagicMock()
    mock_bigquery_client.from_service_account_info.return_value = mock_client

    mock_query_job = MagicMock()
    mock_query_job.result.return_value.to_dataframe.return_value = pd.DataFrame({
        'unique_id': ['id1', 'id2'],
        'data': ['{"articles": [{"title": "title1", "description": "desc1"}]}', '{"articles": [{"title": "title2", "description": "desc2"}]}'],
        'company': ['Company1', 'Company2']
    })
    mock_client.query.return_value = mock_query_job

    df, ids = get_raw_news_from_big_query()

    assert not df.empty
    assert ids == "'id1', 'id2'"

# Test for update_is_processed


@patch('clean_news.bigquery.Client')
def test_update_is_processed(mock_bigquery_client):
    mock_client = MagicMock()
    mock_bigquery_client.from_service_account_info.return_value = mock_client

    mock_job = MagicMock()
    mock_client.query.return_value = mock_job

    result = update_is_processed("'id1', 'id2'")

    mock_client.query.assert_called_once_with(
        "UPDATE `tomastestproject-433206.testdb_1.raw_news_meta_data` "
        "SET is_processed = TRUE "
        "WHERE unique_id IN ('id1', 'id2');"
    )
    assert result == "'id1', 'id2'"

# Test for clean_news


def test_clean_news():
    raw_data = pd.DataFrame({
        'unique_id': ['id1', 'id2'],
        'data': ['{"articles": [{"title": "title1", "description": "desc1"}]}', '{"articles": [{"title": "title2", "description": "desc2"}]}'],
        'company': ['Company1', 'Company2']
    })
    cleaned_df = clean_news(raw_data)

    assert cleaned_df.shape[0] == 2
    assert 'title' in cleaned_df.columns
    assert 'score_description' in cleaned_df.columns

# Test for predict_sentiment


def test_predict_sentiment():
    df = pd.DataFrame({
        'title': ['Positive title', 'Neutral title', 'Negative title'],
        'description': ['Good description', 'Neutral description', 'Bad description']
    })
    predict_sentiment(df)

    assert df['score_title'][0] > 0  # Positive sentiment
    assert df['score_description'][0] > 0  # Positive sentiment
    assert df['score_title'][1] == 0  # Neutral sentiment
    assert df['score_description'][1] == 0  # Neutral sentiment
    assert df['score_title'][2] < 0  # Negative sentiment
    assert df['score_description'][2] < 0  # Negative sentiment

# Test for write_clean_news_to_bq


@patch('clean_news.bigquery.Client')
def test_write_clean_news_to_bq(mock_bigquery_client):
    mock_client = MagicMock()
    mock_bigquery_client.from_service_account_info.return_value = mock_client

    test_df = pd.DataFrame({
        'author': ['Author1'],
        'description': ['Description1'],
        'pub_date': [pd.Timestamp('2024-01-01')],
        'title': ['Title1'],
        'url': ['http://example.com'],
        'source_name': ['Source1'],
        'company': ['Company1'],
        'score_description': [0.5],
        'score_title': [0.5]
    })

    result = write_clean_news_to_bq(test_df)

    mock_client.load_table_from_dataframe.assert_called_once_with(
        test_df, 'tomastestproject-433206.testdb_1.clean_news_copy')
    assert 'rader sparades till clean_news_copy' in result

# Test for transfer_ids_to_meta_data


@patch('clean_news.bigquery.Client')
def test_transfer_ids_to_meta_data(mock_bigquery_client):
    mock_client = MagicMock()
    mock_bigquery_client.from_service_account_info.return_value = mock_client

    mock_query_job = MagicMock()
    mock_query_job.result.return_value.num_dml_affected_rows = 2
    mock_client.query.return_value = mock_query_job

    rows_inserted = transfer_ids_to_meta_data()

    expected_query = (
        "INSERT INTO `tomastestproject-433206.testdb_1.raw_news_meta_data` "
        "(unique_id, is_processed) "
        "SELECT unique_id, FALSE "
        "FROM `tomastestproject-433206.testdb_1.raw_news_data` "
        "WHERE unique_id NOT IN ("
        "SELECT unique_id "
        "FROM `tomastestproject-433206.testdb_1.raw_news_meta_data`)"
    )
    mock_client.query.assert_called_once_with(expected_query)
    assert rows_inserted == 2
