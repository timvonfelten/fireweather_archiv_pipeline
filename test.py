import pandas as pd

# Load the parquet file into a dataframe
df = pd.read_parquet('fireweather_archive_warnregions.parquet')
# df = pd.read_parquet('modified_fireweather_archiv.parquet')


# Display the head of the dataframe
print(df.head())

# Show the end of the dataframe
print(df.tail())

# # Remove entries where date starts with 19, 200, or 201
# df = df[~df['date'].astype(str).str.startswith(('19', '200', '201'))]

# # Display the updated dataframe
# print(df.head())

# # Save the updated dataframe as a new parquet file
# df.to_parquet('/Users/timvonfelten/LocalStorage/GitHub/waldbrand_pipeline/fireweather_archiv_pipeline/modified_fireweather_archiv.parquet')