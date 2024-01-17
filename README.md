# CASBUSI-Anonymize

This repository contains scripts for processing and anonymizing ultrasound image data from Mayo Clinic. The scripts are designed to handle and modify large sets of medical data while maintaining patient confidentiality. There are two primary Python scripts: `prepare_raw_data.py` and `anonymize_data.py`.

## Requirements

- Python 3.x or higher.
- To install dependencies, run: `pip install -r requirements.txt`.

## Scripts Overview

### 1. `prepare_raw_data.py`

This script filters and processes raw data from `.csv` files, specifically for medical imaging data from Mayo Clinic. Key operations include:

- Choosing relevant columns and renaming them for clarity.
- Organizing the data by sorting and reindexing.
- Correlating biopsy entries with corresponding examination records.
- Generating a streamlined dataset, retaining only pertinent information for subsequent analysis and use.

### 2. `anonymize_data.py`

This script de-identifies and anonymizes DICOM files, a common format for medical imaging data. It performs the following functions:

- Unzips DICOM files from a specified directory.
- Removes sensitive patient information from DICOM metadata, images, and videos.
- Optionally saving anonymized images in PNG format for debugging.

### 2. Additional `/Tools/`

This folder provides you with a toolbox of scripts that you may or may not use to assist in anonymizing or analyzing the raw data.

- `anon_map_validation.py`: Makes sure the raw data is consistent with our `master_anon_map` output.
- `dicom_debug.py`: Used for displaying dicom images to find errors during processing.
- `get_stats`: Finds statistics on the `filtered_data`.
- `xlsx_to_csv.py`: File converter.

## Usage

1. **Data Preparation**: 
   - Place the single raw `/input_data/data_complete.csv` data file in the specified input directory.
   - Ensure DICOM files are in the appropriate directory for unzipping and processing.

2. **Run `filter_raw_data.py`**: 
   - Execute this script first to process and filter the raw `/input_data/data_complete.csv` data.
   - This script will output 4 files:
      - `/input_data/filtered_data.csv`, this is the immediate filtered data from the main input file, just present for debugging.
      - `/maps/master_anon_map.csv` and `/maps/master_biop_map.csv` are used to find identified ids from anonymized ids. Keep these files in a safe location.  
      - `/output/total_cases_anon.csv` is the data file that will be used by the [CADBUSI-Database](https://github.com/Poofy1/CADBUSI-Database)

3. **Run `anonymize_data.py`**: 
   - After the previous script finishes processing, use this script to de-identify the DICOM files.
   - Specify the zip file directory and your desired output locations.
   - This script will output 2 folders:
      - A folder of unzipped dicom files, these are still identifiable.
      - A folder of anonymized dicom files that will be used by the [CADBUSI-Database](https://github.com/Poofy1/CADBUSI-Database)
