import os
import pandas as pd
import requests
import zipfile
import sqlite3
import io

def ensure_directory(path):
    if not os.path.exists(path):
        try:
            os.makedirs(path)
        except Exception as e:
            print(f"Error creating directory {path}: {e}")
            exit(1)

def download_and_process_dataset1(url, output_folder, db_path):
    ensure_directory(output_folder)
    response = requests.get(url)
    with zipfile.ZipFile(io.BytesIO(response.content)) as zip_file:
        zip_file.extractall(output_folder)
    csv_filename = os.path.join(output_folder, zip_file.namelist()[1])
    df = pd.read_csv(csv_filename, on_bad_lines='skip')
    df.drop(columns=['Country', 'Location'], inplace=True)
    df = df.loc[~(df==0).all(axis=1)]
    df.dropna(inplace=True)
    ensure_directory(os.path.dirname(db_path))
    conn = sqlite3.connect(db_path)
    df.to_sql('climate_data', conn, if_exists='replace', index=False)
    conn.close()

def download_and_process_dataset2(url, output_folder, db_path):
    ensure_directory(output_folder)
    response = requests.get(url)
    with zipfile.ZipFile(io.BytesIO(response.content)) as zip_file:
        zip_file.extractall(output_folder)
    csv_filename = os.path.join(output_folder, zip_file.namelist()[0])
    df = pd.read_csv(csv_filename, on_bad_lines='skip')
    df.drop(columns=['Area', 'IPPU', 'Fires in humid tropical forests', 'Average Temperature °C'], inplace=True)
    df = df.loc[~(df==0).all(axis=1)]
    df.dropna(inplace=True)
    ensure_directory(os.path.dirname(db_path))
    conn = sqlite3.connect(db_path)
    df.to_sql('Agrofood_co2_emission', conn, if_exists='replace', index=False)
    conn.close()

def main():
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # Moves up two levels from current script's directory
    data_dir = os.path.join(base_dir, 'data')
    project_dir = os.path.join(base_dir, 'project')

    climate_url = 'https://storage.googleapis.com/kaggle-data-sets/3323659/5784613/bundle/archive.zip?X-Goog-Algorithm=GOOG4-RSA-SHA256&X-Goog-Credential=gcp-kaggle-com%40kaggle-161607.iam.gserviceaccount.com%2F20240522%2Fauto%2Fstorage%2Fgoog4_request&X-Goog-Date=20240522T143858Z&X-Goog-Expires=259200&X-Goog-SignedHeaders=host&X-Goog-Signature=6cba02bca5e1652a8c65a17a0c99f1e7b5a376acb987a93075475a4aa689bb5f04520387cea92cf40636bfa8ac72bfc4661b41dbf2b37f3c1b3716fd04d717bee9988fed362ef81105cb5e841418834eaa9c31b361e3e4bc6f657d89676bad218168377fb6dc23db81b61737ca2143e71a1249b3075658661450a356ee01eea390ac6bcc22e69c700949e6808f26490249141a26fd392afb92c53442d5c88372f3e81ea785697daaeeca9b0b227fcf642646dd595bde70697735d84ba55ea0a029eccefc56268e8f14bd67b98a342b90f7582f9a435706edd0100979be9cd75e86e10b01106df6fe305b00e019cf6cbc3f0fc69122ebb8e8582814b2e9be2199'

    co2_url = 'https://storage.googleapis.com/kaggle-data-sets/3526635/6149256/bundle/archive.zip?X-Goog-Algorithm=GOOG4-RSA-SHA256&X-Goog-Credential=gcp-kaggle-com%40kaggle-161607.iam.gserviceaccount.com%2F20240522%2Fauto%2Fstorage%2Fgoog4_request&X-Goog-Date=20240522T143913Z&X-Goog-Expires=259200&X-Goog-SignedHeaders=host&X-Goog-Signature=7a8b4a5fa703177597b1bb61e6238026230311cc44226e6119824a87132538b6beebf7b60d24cae7ea544889befed13971e01ad311d0eac19328fc35786651b88860ff88cd253744548f4772ae7adf8593d78fb79a79e4d336568f2d43881c07280378518b8a14bb1f70f8f51b5663cbb05b39b7ebdecc4f68cf7616231503d27db9bd9a2b6d9770a5825e859adc88f0bf19e3bdd3c1d5be870d3308158f3cd84b5977a7f471ae31d89ca4810f5811f01b3f354c8b9dacb421b6c3770351869434d17731f6f28f58203a7deb23cfb8b9685ad3bb7f0e178bbbdae1808fdfc1850c2f69360e1ce4d26877198f6850bdd6f788ca1e012d93a7da66969981514b08'


    download_and_process_dataset1(climate_url, os.path.join(project_dir, 'climate_data'), os.path.join(data_dir, 'climate_data.sqlite'))
    download_and_process_dataset2(co2_url, os.path.join(project_dir, 'co2_data'), os.path.join(data_dir, 'Agrofood_co2_emission.sqlite'))

if __name__ == "__main__":
    main()

