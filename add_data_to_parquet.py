import rasterio
import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime, timedelta
import os
import geopandas as gpd
from rasterio.features import rasterize
from rasterio.enums import MergeAlg
from concurrent.futures import ProcessPoolExecutor, as_completed

# Entfernen Sie das Dictionary index_folders, da es nicht mehr benötigt wird
def get_latest_date(parquet_file):
    table = pq.read_table(parquet_file)
    df = table.to_pandas()
    return pd.to_datetime(df['date']).max().date()  # Konvertiere zu date-Objekt

def process_date(date, region_masks, output_folder):
    date_str = date.strftime("%Y%m%d")
    print(f"Verarbeite Datum: {date_str}")
    temp_data = []

    index_names = ['temperature', 'bui', 'dc', 'dmc', 'dsr', 'ffmc', 'fwi', 'gfmc', 'isi',
                   'mixr', 'precipitation', 'radiation', 'relative_humidity', 'sdmc',
                   't_msl', 'wind_speed']

    for index_name in index_names:
        file_path = os.path.join(output_folder, f"{index_name}{date_str}.tif")

        if os.path.exists(file_path):
            with rasterio.open(file_path) as src:
                band = src.read(1)
                for region_id, region_mask in region_masks.items():
                    masked_data = band[region_mask == 1]
                    if masked_data.size > 0:
                        mean_value = np.round(np.mean(masked_data), decimals=1)
                        temp_data.append({
                            'date': date_str,
                            'region_id': region_id,
                            'index_name': index_name,
                            'value': mean_value
                        })
        else:
            print(f"Datei nicht gefunden: {file_path}")
            for region_id in region_masks.keys():
                temp_data.append({
                    'date': date_str,
                    'region_id': region_id,
                    'index_name': index_name,
                    'value': np.nan
                })

    return temp_data

def process_new_data(start_date, end_date, region_masks, output_folder):
    all_data = []
    current_date = start_date
    while current_date <= end_date:
        temp_data = process_date(current_date, region_masks, output_folder)
        all_data.extend(temp_data)
        current_date += timedelta(days=1)

    return pd.DataFrame(all_data)

def update_parquet_file(existing_file, new_data, new_file_path):
    # Read existing data
    existing_table = pq.read_table(existing_file)
    existing_df = existing_table.to_pandas()

    # Combine existing and new data
    combined_df = pd.concat([existing_df, new_data], ignore_index=True)

    # Remove duplicates based on date, region_id, and index_name
    combined_df = combined_df.drop_duplicates(subset=['date', 'region_id', 'index_name'], keep='last')

    # Sort the dataframe
    combined_df = combined_df.sort_values(['date', 'region_id', 'index_name'])

    # Pivot the dataframe
    pivoted_df = combined_df.pivot_table(
        values='value', 
        index=['date', 'region_id'], 
        columns='index_name'
    ).reset_index()

    # Convert to PyArrow Table
    table = pa.Table.from_pandas(pivoted_df)

    # Write the table with snappy compression
    pq.write_table(table, new_file_path, row_group_size=143*64, compression='snappy')

if __name__ == "__main__":
    existing_parquet_file = 'fireweather_archive_warnregions.parquet'
    output_folder = 'output'
    new_file_path = 'updated_fireweather_archive_warnregions.parquet'

    # Hole das neueste Datum aus der bestehenden Parquet-Datei
    latest_date = get_latest_date(existing_parquet_file)

    print(f"Neuestes Datum in der Parquet-Datei: {latest_date}")

    # Setze das Startdatum für neue Daten (ein Tag nach dem neuesten Datum)
    start_date = latest_date + timedelta(days=1)

    # Setze das Enddatum (heute)
    end_date = datetime.now().date()

    # Lesen der Forest Mask
    forest_mask_path = 'data/waldmaske_mit_lichtenstein.tif'
    with rasterio.open(forest_mask_path) as src:
        forest_mask = src.read(1)
        transform = src.transform

    # Read the regions and create masks (assuming you have this data)
    geojson_path = 'data/gefahren-waldbrand_warnung_2056.geojson'
    regions = gpd.read_file(geojson_path)
    region_masks = {}
    for idx, region in regions.iterrows():
        region_mask = rasterize(
            [(region.geometry, 1)],
            out_shape=forest_mask.shape,
            transform=transform,
            fill=0,
            all_touched=True,
            merge_alg=MergeAlg.replace
        )
        region_masks[region['region_id']] = region_mask & forest_mask

    # Verarbeite neue Daten
    new_data = process_new_data(start_date, end_date, region_masks, output_folder)

    # Aktualisiere die Parquet-Datei
    update_parquet_file(existing_parquet_file, new_data, new_file_path)

    print(f"Aktualisierte Parquet-Datei wurde erstellt: {new_file_path}")