import os
import pandas as pd
import sqlite3
import kaggle
import tempfile

def setup_kaggle():
    os.environ['KAGGLE_USERNAME'] = 'sinarazi'
    os.environ['KAGGLE_KEY'] = '0865f43ba5a8d25429b4ff59edda698d'

def download_and_extract_dataset(dataset_id, temp_dir):
    """Download and extract a Kaggle dataset into a specified temporary folder."""
    try:
        kaggle.api.dataset_download_files(dataset_id, path=temp_dir, unzip=True)
        print(f'Dataset downloaded and extracted to {temp_dir}')
    except Exception as e:
        print(f"Error downloading and extracting dataset {dataset_id}: {e}")
        return False
    return True

def process_weather_data(temp_dir, output_folder):
    """Process weather data from a specified folder."""
    try:
        csv_filename = os.path.join(temp_dir, 'london_weather.csv')
        df = pd.read_csv(csv_filename)
        original_rows = len(df)

        df['date'] = pd.to_datetime(df['date'], format='%Y%m%d')
        df = df[df['date'].dt.day == 1]  # Keep only the first day of each month
        df['date'] = df['date'].dt.date  # Convert to date only

        df = df[(df['date'] >= pd.to_datetime('2008-01-01').date()) & (df['date'] <= pd.to_datetime('2018-12-01').date())]
        filtered_rows = len(df)
        print(f"Weather Dataset - Original: {original_rows}, Filtered: {filtered_rows}")

        # Save to SQLite
        sqlite_path = os.path.join(output_folder, 'London_weather.sqlite')
        conn = sqlite3.connect(sqlite_path)
        df.drop(columns=['snow_depth'], inplace=True)
        df.to_sql('weather_data', conn, if_exists='replace', index=False)
        conn.close()
        return df
    except Exception as e:
        print(f"Error processing weather data: {e}")
        return None

def process_air_quality_data(temp_dir, output_folder):
    """Process air quality data from a specified folder."""
    try:
        csv_filename = os.path.join(temp_dir, 'data', 'time-of-day-per-month.csv')
        df = pd.read_csv(csv_filename)
        original_rows = len(df)

        df.rename(columns={'Month (text)': 'date'}, inplace=True)  # Rename the column
        df['date'] = pd.to_datetime(df['date'] + ' ' + df['GMT'])
        df = df[df['date'].dt.hour == 14]  # Select only the 14:00 hour
        df['date'] = df['date'].dt.date  # Keep only the date part

        # Shorter column names
        new_column_names = {
            'London Mean Roadside Nitric Oxide (ug/m3)': 'Roadside_NO',
            'London Mean Roadside Nitrogen Dioxide (ug/m3)': 'Roadside_NO2',
            'London Mean Roadside Oxides of Nitrogen (ug/m3)': 'Roadside_NOx',
            'London Mean Roadside Ozone (ug/m3)': 'Roadside_O3',
            'London Mean Roadside PM10 Particulate (ug/m3)': 'Roadside_PM10',
            'London Mean Roadside PM2.5 Particulate (ug/m3)': 'Roadside_PM2_5',
            'London Mean Roadside Sulphur Dioxide (ug/m3)': 'Roadside_SO2',
            'London Mean Background Nitric Oxide (ug/m3)': 'Background_NO',
            'London Mean Background Nitrogen Dioxide (ug/m3)': 'Background_NO2',
            'London Mean Background Oxides of Nitrogen (ug/m3)': 'Background_NOx',
            'London Mean Background Ozone (ug/m3)': 'Background_O3',
            'London Mean Background PM10 Particulate (ug/m3)': 'Background_PM10',
            'London Mean Background PM2.5 Particulate (ug/m3)': 'Background_PM2_5',
            'London Mean Background Sulphur Dioxide (ug/m3)': 'Background_SO2'
        }
        df.rename(columns=new_column_names, inplace=True)

        df = df[(df['date'] >= pd.to_datetime('2008-01-01').date()) & (df['date'] <= pd.to_datetime('2018-12-01').date())]
        filtered_rows = len(df)
        print(f"Time of Day Dataset - Original: {original_rows}, Filtered: {filtered_rows}")

        # Save to SQLite
        sqlite_path = os.path.join(output_folder, 'London_pollution.sqlite')
        conn = sqlite3.connect(sqlite_path)
        df.drop(columns=['GMT'], inplace=True)  # Drop the GMT column before saving
        df.drop(columns=['Roadside_NO'], inplace=True)
        df.drop(columns=['Background_NO'], inplace=True)
        df.to_sql('time_of_day_data', conn, if_exists='replace', index=False)
        conn.close()

        return df
    except Exception as e:
        print(f"Error processing air quality data: {e}")
        return None

def main():
    setup_kaggle()  

    data_dir = '../data'
    dataset1_id = 'emmanuelfwerr/london-weather-data'
    dataset2_id = 'zsn6034/london-air-quality'

    with tempfile.TemporaryDirectory() as temp_dir1, tempfile.TemporaryDirectory() as temp_dir2:
        if not download_and_extract_dataset(dataset1_id, temp_dir1):
            print(f"Failed to download dataset {dataset1_id}")
            return
        if not download_and_extract_dataset(dataset2_id, temp_dir2):
            print(f"Failed to download dataset {dataset2_id}")
            return

        df1 = process_weather_data(temp_dir1, data_dir)
        df2 = process_air_quality_data(temp_dir2, data_dir)

        if df1 is not None and df2 is not None:
            df_combined = pd.merge(df1, df2, on='date', how='inner')
            combined_db_path = os.path.join(data_dir, 'combined_London_climate.sqlite')
            try:
                conn = sqlite3.connect(combined_db_path)
                df_combined.to_sql('combined_weather_data', conn, if_exists='replace', index=False)
                conn.close()
                print(f"Combined data loaded into SQLite. Rows: {len(df_combined)}")
            except Exception as e:
                print(f"Error saving combined data to SQLite: {e}")
        else:
            print("Error: Failed to process one or more datasets")

if __name__ == "__main__":
    main()
