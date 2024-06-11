import os
import sqlite3
import pytest
import pandas as pd
import kaggle

# Like the pipeline, it is defined in github secret variables.
def setup_kaggle():
    """
    Set up Kaggle API credentials from environment variables.
    """
    os.environ['KAGGLE_USERNAME'] = os.getenv('KAGGLE_USERNAME')
    os.environ['KAGGLE_KEY'] = os.getenv('KAGGLE_KEY')

setup_kaggle()

expected_files = [
    "./data/London_pollution.sqlite",
    "./data/London_weather.sqlite",
    "./data/combined_London_climate.sqlite"
]

expected_tables = {
    "./data/London_pollution.sqlite": ["time_of_day_data"],
    "./data/London_weather.sqlite": ["weather_data"],
    "./data/combined_London_climate.sqlite": ["combined_weather_data"]
}

# Critical columns and their expected data types for integrity tests
expected_columns_types = {
    "./data/London_pollution.sqlite": {
        "time_of_day_data": {
            "date": "object",
            "Roadside_NO2": "float64",
            "Roadside_PM10": "float64"
        }
    },
    "./data/London_weather.sqlite": {
        "weather_data": {
            "date": "object",
            "max_temp": "float64",
            "precipitation": "float64"
        }
    }
}

# Test to check the existence of the output files
@pytest.mark.parametrize("filepath", expected_files)
def test_file_existence(filepath):
    assert os.path.isfile(filepath), f"File {filepath} does not exist."

# Test to check the tables within each SQLite file
@pytest.mark.parametrize("filepath,tables", expected_tables.items())
def test_sqlite_tables(filepath, tables):
    conn = sqlite3.connect(filepath)
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    existing_tables = [row[0] for row in cursor.fetchall()]
    conn.close()
    for table in tables:
        assert table in existing_tables, f"Table {table} not found in {filepath}."

# Test to check the integrity of data in SQLite files
@pytest.mark.parametrize("filepath,tables_columns", expected_columns_types.items())
def test_data_integrity(filepath, tables_columns):
    conn = sqlite3.connect(filepath)
    for table, columns_types in tables_columns.items():
        df = pd.read_sql_query(f"SELECT * FROM {table}", conn)
        for column, dtype in columns_types.items():
            assert column in df.columns, f"Column {column} not found in table {table} of {filepath}"
            assert df[column].dtype == dtype, f"Column {column} in table {table} of {filepath} has incorrect type {df[column].dtype}, expected {dtype}"
    conn.close()

# Test to check Kaggle credentials
def test_kaggle_credentials():
    username = os.getenv('KAGGLE_USERNAME')
    key = os.getenv('KAGGLE_KEY')
    assert username is not None, "Kaggle username not set"
    assert key is not None, "Kaggle key not set"

# Test to check Kaggle dataset URLs
@pytest.mark.parametrize("dataset_id", [
    "emmanuelfwerr/london-weather-data",
    "zsn6034/london-air-quality"
])
def test_kaggle_dataset_exists(dataset_id):
    try:
        kaggle.api.dataset_download_files(dataset_id, path='.', unzip=False)
    except Exception as e:
        pytest.fail(f"Dataset {dataset_id} could not be accessed. Error: {e}")

if __name__ == "__main__":
    pytest.main()
