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

def get_latest_date(parquet_file):
    table = pq.read_table(parquet_file)
    df = table.to_pandas()
    return pd.to_datetime(df['date']).max().date()

def process_date(date, region_masks, output_folder):
    date_str = date.strftime("%Y%m%d")
    print(f"Processing date: {date_str}")
    temp_data = []

    index_names = ['temperature', 'bui', 'dc', 'dmc', 'dsr', 'ffmc', 'fwi', 'gfmc', 'isi',
                   'mixr', 'precipitation', 'radiation', 'relative_humidity', 'sdmc',
                   't_msl', 'wind_speed']

    for region_id in region_masks.keys():
        row_data = {'date': date_str, 'region_id': region_id}
        for index_name in index_names:
            file_path = os.path.join(output_folder, f"{index_name}{date.strftime('%Y%m%d')}.tif")
            if os.path.exists(file_path):
                with rasterio.open(file_path) as src:
                    band = src.read(1)
                    region_mask = region_masks[region_id]
                    masked_data = band[region_mask == 1]
                    if masked_data.size > 0:
                        mean_value = np.round(np.mean(masked_data), decimals=1)
                        row_data[index_name] = mean_value
                    else:
                        row_data[index_name] = np.nan
                # print(f"Processed: {file_path}")
                # print(f"Value for region {region_id}, index {index_name}: {row_data[index_name]}")
            else:
                row_data[index_name] = np.nan
                print(f"File not found: {file_path}")
        temp_data.append(row_data)

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
    print(f"new data: {new_data}")
    # Read existing data
    existing_table = pq.read_table(existing_file)
    existing_df = existing_table.to_pandas()

    # Combine existing and new data
    combined_df = pd.concat([existing_df, new_data], ignore_index=True)

    # Remove duplicates based on date and region_id
    combined_df = combined_df.drop_duplicates(subset=['date', 'region_id'], keep='last')

    # Sort the dataframe
    combined_df = combined_df.sort_values(['date', 'region_id'])

    # Convert to PyArrow Table
    table = pa.Table.from_pandas(combined_df)

    # Write the table with snappy compression
    pq.write_table(table, new_file_path, row_group_size=143*64, compression='snappy')


if __name__ == "__main__":
    existing_parquet_file = 'fireweather_archive_warnregions.parquet'
    output_folder = 'output'
    new_file_path = 'fireweather_archive_warnregions.parquet'

    # Get the latest date from the existing Parquet file
    latest_date = get_latest_date(existing_parquet_file)

    print(f"Latest date in the Parquet file: {latest_date}")

    # Set the start date for new data (one day after the latest date)
    start_date = latest_date + timedelta(days=1)

    # Set the end date (today)
    end_date = datetime.now().date()

    # Read the Forest Mask
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

    # Process new data
    new_data = process_new_data(start_date, end_date, region_masks, output_folder)

    # Update the Parquet file
    update_parquet_file(existing_parquet_file, new_data, new_file_path)

    print(f"Updated Parquet file has been created: {new_file_path}")