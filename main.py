import io
import os
import requests
import re
import time
import pandas as pd
from PIL import Image
from backend import *

env = os.path.dirname(os.path.abspath(__file__))




# List all files in the directory "Datamart_input/" and take the first one
datamart_directory = f'{env}/datamart_input/'
datamart_files = os.listdir(datamart_directory)
if datamart_files:
    datamart_file = datamart_directory + datamart_files[0]
else:
    print('No files found in datamart directory', datamart_directory)
    datamart_file = None  # Or assign a default value


result_file_path = upload_datamart(datamart_file, f"{env}/datamart_output")


# Find notion and zip files
zip_filenames = [f for f in os.listdir(f"{env}/zip_files") if f.endswith('.zip')]
notion_files = [f for f in os.listdir(f"{env}/datamart_output") if f.endswith('.csv')]

# Process notion files and zip files
upload_notion(zip_filenames[0], notion_files[0], f"{env}/datamart_output", f"{env}/zip_files", f"{env}/notion_output")







db_dicom_files = fetch_dicom_files()
df_list = []
for d in db_dicom_files:
    new_d = d.copy()
    new_d.pop('metadata')
    df_list.append(new_d)

db_dicom_files_df = pd.DataFrame(df_list)
print(db_dicom_files_df)



result = export_data()
print(result)