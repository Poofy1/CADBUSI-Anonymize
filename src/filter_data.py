import os
import pandas as pd
from tqdm import tqdm

# Get the current script directory and go back one directory
env = os.path.dirname(os.path.abspath(__file__))
env = os.path.dirname(env)  # Go back one directory

def check_assumed_benign(final_df):
    """
    Check for benign cases based on 18-month follow-up.
    Only considers:
    1. Cases with BI-RADS '1' or '2'
    2. Cases with no biopsy from -30 to +120 days around the US date
    3. Cases with no non-benign (non 1-2) BI-RADS in 24 month follow-up
    4. Cases with no 'MALIGNANT' in path_interpretation within 15 months
    """
    today = pd.Timestamp.now()
    
    # Pre-compute the eligible US records with BI-RADS 1 or 2 to avoid processing irrelevant rows
    us_mask = (final_df['MODALITY'] == 'US') & (final_df['BI-RADS'].isin(['1', '2'])) & pd.notna(final_df['DATE'])
    us_records = final_df[us_mask]
    
    # Only process patients who have eligible US records
    patient_ids = us_records['PATIENT_ID'].unique()
    
    # Create a dictionary to store final interpretation updates
    updates = {}
    
    for patient_id in tqdm(patient_ids, desc="Checking benign based on followup"):
        # Get all records for this patient once and sort them
        patient_records = final_df[final_df['PATIENT_ID'] == patient_id].sort_values('DATE')
        
        # Process only eligible US records for this patient
        patient_us_records = us_records[us_records['PATIENT_ID'] == patient_id]
        
        for idx, row in patient_us_records.iterrows():
            # Skip records that are less than 24 months old
            if (today - row['DATE']).days < 730:
                continue
            
            # Define time windows
            biopsy_window_start = row['DATE'] - pd.Timedelta(days=30)
            biopsy_window_end = row['DATE'] + pd.Timedelta(days=120)
            followup_end = row['DATE'] + pd.Timedelta(days=730)  # 24 months
            malignancy_window_end = row['DATE'] + pd.Timedelta(days=450)  # 15 months
            
            # Filter for records in the biopsy window efficiently
            biopsy_window_records = patient_records[
                (patient_records['DATE'] >= biopsy_window_start) &
                (patient_records['DATE'] <= biopsy_window_end)
            ]
            
            # Skip if any biopsies exist in this window
            if 'is_biopsy' in biopsy_window_records.columns and (biopsy_window_records['is_biopsy'] == 'T').any():
                continue
            
            # Filter for follow-up records efficiently
            followup_records = patient_records[
                (patient_records['DATE'] > row['DATE']) &
                (patient_records['DATE'] <= followup_end)
            ]
            
            # Check for non-benign BI-RADS in follow-up period efficiently
            if 'BI-RADS' in followup_records.columns:
                non_benign_mask = followup_records['BI-RADS'].notna() & ~followup_records['BI-RADS'].isin(['1', '2'])
                if non_benign_mask.any():
                    continue
            
            # Filter for records in malignancy window
            malignancy_records = patient_records[
                (patient_records['DATE'] > row['DATE']) &
                (patient_records['DATE'] <= malignancy_window_end)
            ]
            
            # Check for 'MALIGNANT' in path_interpretation efficiently
            if 'path_interpretation' in malignancy_records.columns:
                # Check specifically for non-null values that contain "MALIGNANT"
                has_malignancy = False
                for _, malignancy_record in malignancy_records.iterrows():
                    if pd.notna(malignancy_record.get('path_interpretation')) and 'MALIGNANT' in str(malignancy_record['path_interpretation']).upper():
                        has_malignancy = True
                        break
                
                if has_malignancy:
                    continue
            
            # If all checks pass, mark for update
            updates[idx] = 'BENIGN1'
    
    # Apply all updates at once
    for idx, value in updates.items():
        final_df.at[idx, 'final_interpretation'] = value
    
    return final_df


def check_malignant_from_biopsy(final_df):
    """
    Check for malignancy indicators by marking rows as MALIGNANT1 
    if BI-RADS = 6 and MODALITY is 'US', for cases without interpretation.
    """
    us_birads6_rows = final_df[
        (pd.notna(final_df.get('MODALITY'))) & 
        (final_df['MODALITY'] == 'US') & 
        (pd.notna(final_df.get('BI-RADS'))) & 
        (final_df['BI-RADS'] == '6')
    ].index
    
    for idx in tqdm(us_birads6_rows, desc="Checking BI-RADS 6 cases"):
        row = final_df.loc[idx]
        if pd.isna(row['final_interpretation']) or row['final_interpretation'] == '':
            final_df.at[idx, 'final_interpretation'] = 'MALIGNANT1'
    
    return final_df


def check_from_next_diagnosis(final_df, days=240):
    """
    For 'US' rows with empty final_interpretation, check if the next chronological 
    record with a path_interpretation within 'days' is 'BENIGN' or 'MALIGNANT'.
    
    For MALIGNANT cases: Set final_interpretation to 'MALIGNANT2' only if:
    1. The laterality matches between the US study and the pathology
    2. At least one record in the date range has 'is_us_biopsy' = 'T'
    
    For BENIGN cases: Set final_interpretation to 'BENIGN2' if:
    1. The laterality matches between the US study and the pathology
    
    Special case: If Study_Laterality is 'BILATERAL', consider any future laterality,
    and if any MALIGNANT is present within the time frame, set to 'MALIGNANT2'
    (still requiring at least one 'is_us_biopsy' = 'T').
    """
    for patient_id in tqdm(final_df['PATIENT_ID'].unique(), desc="Checking diagnosis from next record"):
        patient_mask = final_df['PATIENT_ID'] == patient_id
        patient_records = final_df[patient_mask].copy()
        
        # Only proceed if there are multiple records for this patient
        if len(patient_records) < 2:
            continue
        
        # Sort records by date
        patient_records = patient_records.sort_values('DATE')
        
        # Iterate through US records with empty final_interpretation
        for idx, current_row in patient_records.iterrows():
            # Check if current row is US with empty final_interpretation
            if (pd.notna(current_row.get('MODALITY')) and 
                current_row['MODALITY'] == 'US' and
                (pd.isna(current_row['final_interpretation']) or 
                 current_row['final_interpretation'] == '')):
                
                # Get current date and calculate future date
                if pd.isna(current_row['DATE']):
                    continue
                    
                # Skip if Study_Laterality is missing
                if pd.isna(current_row.get('Study_Laterality')):
                    continue
                
                current_date = current_row['DATE']
                future_date = current_date + pd.Timedelta(days=days) 
                current_laterality = current_row['Study_Laterality']
                
                # Find future records within the time window
                future_records = patient_records[
                    (patient_records['DATE'] > current_date) & 
                    (patient_records['DATE'] <= future_date)
                ]
                
                # Check if there's at least one record with 'is_us_biopsy' = 'T' in the date range
                has_us_biopsy = any(
                    (record['is_us_biopsy'] == 'T') 
                    for _, record in future_records.iterrows() 
                    if pd.notna(record.get('is_us_biopsy'))
                )
                
                # Handle BILATERAL case differently
                if current_laterality == 'BILATERAL':
                    # Check all future records with pathology interpretation
                    path_interpretations = []
                    for _, future_row in future_records.iterrows():
                        if pd.notna(future_row.get('path_interpretation')):
                            path_interpretations.append(future_row['path_interpretation'].upper())
                    
                    # If any MALIGNANT is found and has_us_biopsy, set to MALIGNANT2
                    if 'MALIGNANT' in path_interpretations and has_us_biopsy:
                        final_df.at[idx, 'final_interpretation'] = 'MALIGNANT2'
                    # If no MALIGNANT but at least one BENIGN, set to BENIGN2 (no us_biopsy requirement)
                    elif 'BENIGN' in path_interpretations:
                        final_df.at[idx, 'final_interpretation'] = 'BENIGN2'
                
                # Handle regular laterality matching
                else:
                    # Look for the next record with a valid path_interpretation
                    for _, future_row in future_records.iterrows():
                        if pd.notna(future_row.get('path_interpretation')):
                            # Check if pathology laterality exists and matches the study laterality
                            if (pd.notna(future_row.get('Pathology_Laterality')) and 
                                future_row['Pathology_Laterality'] == current_laterality):
                                
                                # Check if benign or malignant
                                path_interp = future_row['path_interpretation'].upper()
                                if path_interp == 'BENIGN':
                                    final_df.at[idx, 'final_interpretation'] = 'BENIGN2'
                                    break  # Stop after finding benign
                                elif path_interp == 'MALIGNANT' and has_us_biopsy:
                                    final_df.at[idx, 'final_interpretation'] = 'MALIGNANT2'
                                    break  # Stop after finding malignant
    
    return final_df

def determine_final_interpretation(final_df):
    """
    Determine final_interpretation for each patient based on specified rules.
    """
    # First check: Identify BENIGN1 cases based on follow-up period
    final_df = check_assumed_benign(final_df)
    
    # Second check: Identify MALIGNANT1 cases from biopsy results for remaining cases
    final_df = check_malignant_from_biopsy(final_df)
    
    # Third check: Identify BENIGN2 cases based on next chronological path_interpretation
    final_df = check_from_next_diagnosis(final_df)
    
    return final_df
















def prepare_dataframes(rad_df, path_df):
    """Prepare and standardize dataframes for combining."""
    
    # Convert Patient_ID to string in both dataframes - use inplace for better performance
    rad_df['PATIENT_ID'] = rad_df['PATIENT_ID'].astype(str)
    path_df['PATIENT_ID'] = path_df['PATIENT_ID'].astype(str)
    
    # Convert date columns and rename in one step
    rad_df['DATE'] = pd.to_datetime(rad_df['RADIOLOGY_DTM'], errors='coerce')
    path_df['DATE'] = pd.to_datetime(path_df['SPECIMEN_RECEIVED_DTM'], errors='coerce')
    
    # Drop columns more efficiently (in-place)
    rad_df.drop('RADIOLOGY_DTM', axis=1, inplace=True)
    path_df.drop('SPECIMEN_RECEIVED_DTM', axis=1, inplace=True)
    
    return rad_df, path_df


def combine_dataframes(rad_df, path_df):
    """Combine radiology and pathology dataframes, keeping pathology on separate rows."""
    # Select only needed columns from path_df to reduce memory usage
    needed_columns = ['PATIENT_ID', 'DATE', 'Pathology_Laterality', 'final_diag', 'path_interpretation']
    path_needed = path_df[needed_columns] if all(col in path_df.columns for col in needed_columns) else path_df
    
    # Create empty DataFrame with the same columns as rad_df
    path_records_df = pd.DataFrame(columns=rad_df.columns)
    
    # Set values for columns that exist in both dataframes
    for col in path_needed.columns:
        if col in path_records_df.columns:
            path_records_df[col] = path_needed[col]
    
    # Concatenate more efficiently with optimized settings
    final_df = pd.concat([rad_df, path_records_df], ignore_index=True, copy=False)
    
    return final_df


def create_final_dataset(rad_df, path_df):
    """Main function to create the final dataset with pathology records on separate rows."""
    print("Creating Final Dataset")
    
    # Prepare dataframes
    rad_df, path_df = prepare_dataframes(rad_df, path_df)
    
    # Combine dataframes
    final_df = combine_dataframes(rad_df, path_df)
    
    # Determine final interpretation
    final_df = determine_final_interpretation(final_df)
    
    # Save to CSV
    final_df.to_csv(f'{env}/raw_data/combined_dataset_debug.csv', index=False)
    
    # Print statistics
    print(f"Dataset created with {len(final_df)} records")

    
    
    # Filter to keep only rows with 'US' in MODALITY
    final_df_us = final_df[final_df['MODALITY'].str.contains('US', na=False, case=False)]

    # Remove rows with empty ENDPOINT_ADDRESS or empty final_interpretation
    final_df_us = final_df_us[
        final_df_us['ENDPOINT_ADDRESS'].notna() & 
        final_df_us['final_interpretation'].notna()
    ]
    
    # Remove rows with 'incomplete' in the Biopsy column
    final_df_us = final_df_us[~(final_df_us['Biopsy'].str.contains('incomplete', case=False, na=False))]

    # Save the US-only filtered dataset
    os.makedirs(f'{env}/output', exist_ok=True)
    final_df_us.to_csv(f'{env}/output/endpoint_data.csv', index=False)

    # Print statistics
    print(f"Dataset passed with {len(final_df_us)} results")
    
    return final_df



if __name__ == "__main__":
    # Load the parsed radiology and pathology data
    try:
        rad_file_path = f'{env}/raw_data/parsed_radiology.csv'
        path_file_path = f'{env}/raw_data/parsed_pathology.csv'
        
        rad_df = pd.read_csv(rad_file_path)
        print(f"Loaded radiology data with {len(rad_df)} records")
        
        path_df = pd.read_csv(path_file_path)
        print(f"Loaded pathology data with {len(path_df)} records")
        
        # Call the create_final_dataset function
        create_final_dataset(rad_df, path_df)
        
    except FileNotFoundError as e:
        print(f"Error: {e}")
        print("Please make sure you've run the parsing scripts to create the parsed CSV files first.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")