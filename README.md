# CASBUSI-Anonymize

This repository contains scripts for processing and anonymizing medical imaging data from Mayo Clinic. The scripts are designed to handle and modify large sets of medical data while maintaining patient confidentiality. There are two primary Python scripts: `filter_raw_data.py` and `anonymize_data.py`.

## Requirements

- Python 3.x or higher.
- To install dependencies, run: `pip install -r requirements.txt`.

## Scripts Overview

### 1. `filter_raw_data.py`

This script filters and processes raw data from `.csv` files, specifically for medical imaging data from Mayo Clinic. Key operations include:

- Selecting and renaming columns for better understanding.
- Sorting and reindexing the data for consistency.
- Adding new columns based on specific criteria.
- Matching biopsy records to examination records.
- Outputting a filtered dataset with relevant information for further processing.

### 2. `anonymize_data.py`

This script de-identifies and anonymizes DICOM files, a common format for medical imaging data. It performs the following functions:

- Unzipping DICOM files from a specified directory.
- Removing sensitive patient information from DICOM metadata.
- Handling various types of DICOM files, including images and multi-frame files.
- Optionally saving anonymized images in PNG format for visual verification and debugging.

## Usage

1. **Data Preparation**: 
   - Place raw `.csv` data files in the specified input directory.
   - Ensure DICOM files are in the appropriate directory for unzipping and processing.

2. **Run `filter_raw_data.py`**: 
   - Execute this script first to process and filter the raw `.csv` data.
   - The script will generate a cleaned dataset, which is used in the next step.

3. **Run `anonymize_data.py`**: 
   - Use this script to de-identify the DICOM files.
   - The script supports options like saving PNGs for debugging purposes.
   - Ensure to adjust file paths and parameters according to your environment setup.
