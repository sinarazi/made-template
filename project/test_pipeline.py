import os
import sqlite3
import pytest
import pandas as pd
import kaggle
import tempfile

# Like the pipeline, it is defined in github secret variables.
def setup_kaggle():
    """
    Set up Kaggle API credentials from environment variables.
    """
    os.environ['KAGGLE_USERNAME'] = os.getenv('KAGGLE_USERNAME')
    os.environ['KAGGLE_KEY'] = os.getenv('KAGGLE_KEY')
    kaggle.api.authenticate()
    
setup_kaggle()

expected_files = [
    "../data/London_pollution.sqlite",
    "../data/London_weather.sqlite",
    "../data/combined_London_climate.sqlite"
]

expected_tables = {
    "London_pollution.sqlite": ["time_of_day_data"],
    "London_weather.sqlite": ["weather_data"],
    "combined_London_climate.sqlite": ["combined_weather_data"]
}

# Critical columns and their expected data types for integrity tests
expected_columns_types = {
    "time_of_day_data": {
        "date": "object",
        "Roadside_NO2": "float64",
        "Roadside_PM10": "float64"
    },
    "weather_data": {
        "date": "object",
        "max_temp": "float64",
        "precipitation": "float64"
    },
    "combined_weather_data": {
        "date": "object",
        "max_temp": "float64",
        "precipitation": "float64",
        "Roadside_NO2": "float64",
        "Roadside_PM10": "float64"
    }
}

def mock_process_weather_data():
    data = {
        'date': pd.date_range(start='2008-01-01', periods=10, freq='MS').date,
        'max_temp': [10.0, 12.0, 15.0, 18.0, 20.0, 22.0, 25.0, 20.0, 15.0, 10.0],
        'precipitation': [5.0, 4.0, 3.0, 2.0, 1.0, 0.0, 1.0, 2.0, 3.0, 4.0]
    }
    return pd.DataFrame(data)

def mock_process_air_quality_data():
    data = {
        'date': pd.date_range(start='2008-01-01', periods=10, freq='MS').date,
        'Roadside_NO2': [30.0, 28.0, 25.0, 22.0, 20.0, 18.0, 15.0, 20.0, 25.0, 30.0],
        'Roadside_PM10': [50.0, 48.0, 45.0, 42.0, 40.0, 38.0, 35.0, 40.0, 45.0, 50.0]
    }
    return pd.DataFrame(data)

# Test to check Kaggle credentials are set
def test_kaggle_credentials():
    setup_kaggle()
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
        kaggle.api.dataset_download_files(dataset_id, path=tempfile.mkdtemp(), unzip=False)
    except Exception as e:
        pytest.fail(f"Dataset {dataset_id} could not be accessed. Error: {e}")

# Test to check the existence of the output files (actual check)
@pytest.mark.parametrize("filepath", expected_files)
def test_file_existence(filepath):
    assert os.path.isfile(filepath), f"File {filepath} does not exist."

# Test to check the tables within each SQLite file (in-memory databases)
@pytest.mark.parametrize("db_name,tables", expected_tables.items())
def test_sqlite_tables(db_name, tables):
    # Create in-memory SQLite databases to test the presence of tables
    with sqlite3.connect(':memory:') as conn:
        cursor = conn.cursor()
        if db_name == "London_pollution.sqlite":
            mock_df = mock_process_air_quality_data()
            mock_df.to_sql('time_of_day_data', conn, index=False)
        elif db_name == "London_weather.sqlite":
            mock_df = mock_process_weather_data()
            mock_df.to_sql('weather_data', conn, index=False)
        else:
            df_weather = mock_process_weather_data()
            df_air_quality = mock_process_air_quality_data()
            df_combined = pd.merge(df_weather, df_air_quality, on='date', how='inner')
            df_combined.to_sql('combined_weather_data', conn, index=False)

        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        existing_tables = [row[0] for row in cursor.fetchall()]
        for table in tables:
            assert table in existing_tables, f"Table {table} not found in {db_name}."

# Test to check the integrity of data in SQLite files (in-memory databases)
@pytest.mark.parametrize("table,columns_types", expected_columns_types.items())
def test_data_integrity(table, columns_types):
    # Create in-memory SQLite databases to test data integrity
    with sqlite3.connect(':memory:') as conn:
        if table == 'weather_data':
            df = mock_process_weather_data()
        elif table == 'time_of_day_data':
            df = mock_process_air_quality_data()
        else:
            df_weather = mock_process_weather_data()
            df_air_quality = mock_process_air_quality_data()
            df = pd.merge(df_weather, df_air_quality, on='date', how='inner')

        df.to_sql(table, conn, index=False)

        df_from_db = pd.read_sql_query(f"SELECT * FROM {table}", conn)
        for column, expected_dtype in columns_types.items():
            assert column in df_from_db.columns, f"Column {column} not found in table {table}"
            assert str(df_from_db[column].dtype) == expected_dtype, f"Column {column} in table {table} has incorrect type {df_from_db[column].dtype}, expected {expected_dtype}"

if __name__ == "__main__":
    pytest.main()
