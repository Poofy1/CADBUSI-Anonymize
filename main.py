import io
import os
import requests
import re
import time
import pandas as pd
from PIL import Image
from backend import *

env = os.path.dirname(os.path.abspath(__file__))

DATAMART_INPUT = f'{env}/data/csv/datamart/'
DATAMART_OUTPUT = f'{env}/data/csv/notion/'
BACKUP_DIR = f'{env}/data/csv/all_data/'
ZIP_INPUT = f'{env}/data/zip/input/'
ZIP_OUTPUT = f'{env}/data/zip/output/'
FINAL_OUTPUT = f'{env}/final_output/'


# List all files in the directory "Datamart_input/" and take the first one
datamart_files = os.listdir(DATAMART_INPUT)
if datamart_files:
    datamart_file = DATAMART_INPUT + datamart_files[0]
else:
    print('No files found in datamart directory', DATAMART_INPUT)
    datamart_file = None  # Or assign a default value

# Step 1 : Upload datamart file to get notion files
user_input = input("Converting datamart file to notion files? (y/n): ")
if user_input.lower() == "y":
    upload_datamart(datamart_file, DATAMART_OUTPUT, BACKUP_DIR)
    print("Completed")


# Step 2 : Find notion / zip files 
# Add to database
# Unzip zipped file
# Create .dcm file
user_input = input("Process notion and zip data and convert to .json? (y/n): ")
if user_input.lower() == "y":
    zip_filenames = [f for f in os.listdir(ZIP_INPUT) if f.endswith('.zip')]
    notion_files = [f for f in os.listdir(DATAMART_OUTPUT) if f.endswith('.csv')]
    upload_notion(f'{ZIP_INPUT}/{zip_filenames[0]}', notion_files[0], DATAMART_OUTPUT, ZIP_OUTPUT, BACKUP_DIR)
    print("Completed")


"""db_dicom_files = fetch_dicom_files()
df_list = []
for d in db_dicom_files:
    new_d = d.copy()
    new_d.pop('metadata')
    df_list.append(new_d)

db_dicom_files_df = pd.DataFrame(df_list)
print(db_dicom_files_df)"""


# Step 3 : Export Data
user_input = input("Export image, json, and csv data? (y/n): ")
if user_input.lower() == "y":
    export_data(FINAL_OUTPUT, ZIP_OUTPUT)
    print("Completed")