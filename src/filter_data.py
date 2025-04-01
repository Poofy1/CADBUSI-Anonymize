import os
import pandas as pd
from tqdm import tqdm

# Get the current script directory and go back one directory
env = os.path.dirname(os.path.abspath(__file__))
env = os.path.dirname(env)  # Go back one directory

def check_benign_based_on_followup(final_df):
    """
    Check for benign cases based on 18-month follow-up without malignancy indicators.
    """
    today = pd.Timestamp.now()
    trigger_words = ['malignant', 'proven malignancy', 'proven cancer', 'carcinoma']
    
    for patient_id in tqdm(final_df['PATIENT_ID'].unique(), desc="Checking benign based on followup"):
        patient_mask = final_df['PATIENT_ID'] == patient_id
        patient_records = final_df[patient_mask].copy()
        patient_records = patient_records.sort_values('DATE')
        
        for idx, row in patient_records.iterrows():
            if pd.isna(row['DATE']):
                continue
                
            if row['MODALITY'] == 'US':
                if (today - row['DATE']).days < 540:  # Less than 18 months of follow-up
                    continue
                    
                start_date = row['DATE'] - pd.Timedelta(days=12)
                end_date = row['DATE'] + pd.Timedelta(days=540)
                
                records_in_timeframe = patient_records[
                    (patient_records['DATE'] >= start_date) &
                    (patient_records['DATE'] <= end_date)
                ]
                
                has_trigger_words = False
                for _, record in records_in_timeframe.iterrows():
                    if 'Biopsy' in record and pd.notna(record['Biopsy']):
                        biopsy_text = str(record['Biopsy']).lower()
                        if any(word in biopsy_text for word in trigger_words):
                            has_trigger_words = True
                            break
                    
                    if 'path_interpretation' in record and pd.notna(record['path_interpretation']):
                        diagnosis_text = str(record['path_interpretation']).lower()
                        if any(word in diagnosis_text for word in trigger_words):
                            has_trigger_words = True
                            break

                if not has_trigger_words:
                    final_df.at[idx, 'final_interpretation'] = 'BENIGN1'
    
    return final_df


def check_malignant_from_biopsy(final_df):
    """
    Check for malignancy indicators in biopsy results for cases without interpretation,
    but only for rows where MODALITY is 'US'.
    """
    trigger_words = ['malignant', 'cancer', 'carcinoma', 'intermediate', 'malignancy']
    
    us_rows = final_df[(pd.notna(final_df.get('MODALITY'))) & (final_df['MODALITY'] == 'US')].index
    
    for idx in tqdm(us_rows, desc="Checking malignancy from biopsy"):
        row = final_df.loc[idx]
        if pd.isna(row['final_interpretation']) or row['final_interpretation'] == '':
            if 'Biopsy' in row and pd.notna(row['Biopsy']):
                biopsy_text = str(row['Biopsy']).lower()
                if any(word in biopsy_text for word in trigger_words):
                    final_df.at[idx, 'final_interpretation'] = 'MALIGNANT1'
    
    return final_df


def check_from_next_diagnosis(final_df, days=240):
    """
    For 'US' rows with empty final_interpretation, check if the next chronological 
    record with a path_interpretation within 'days' is 'BENIGN' or 'MALIGNANT', and set 
    final_interpretation to 'BENIGN2' or 'MALIGNANT2' if the laterality matches between 
    the US study and the pathology.
    
    Special case: If Study_Laterality is 'BILATERAL', consider any future laterality,
    and if any MALIGNANT is present within the time frame, set to 'MALIGNANT2'.
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
                
                # Handle BILATERAL case differently
                if current_laterality == 'BILATERAL':
                    # Check all future records with pathology interpretation
                    path_interpretations = []
                    for _, future_row in future_records.iterrows():
                        if pd.notna(future_row.get('path_interpretation')):
                            path_interpretations.append(future_row['path_interpretation'].upper())
                    
                    # If any MALIGNANT is found, set to MALIGNANT2
                    if 'MALIGNANT' in path_interpretations:
                        final_df.at[idx, 'final_interpretation'] = 'MALIGNANT2'
                    # If no MALIGNANT but at least one BENIGN, set to BENIGN2
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
                                elif path_interp == 'MALIGNANT':
                                    final_df.at[idx, 'final_interpretation'] = 'MALIGNANT2'
                            
                                break  # Stop after finding the first record with a diagnosis
    
    return final_df

def determine_final_interpretation(final_df):
    """
    Determine final_interpretation for each patient based on specified rules.
    """
    # First check: Identify BENIGN1 cases based on follow-up period
    final_df = check_benign_based_on_followup(final_df)
    
    # Second check: Identify MALIGNANT1 cases from biopsy results for remaining cases
    final_df = check_malignant_from_biopsy(final_df)
    
    # Third check: Identify BENIGN2 cases based on next chronological path_interpretation
    final_df = check_from_next_diagnosis(final_df)
    
    return final_df
















def prepare_dataframes(rad_df, path_df):
    """Prepare and standardize dataframes for combining."""
    # Create copies to avoid modifying originals
    rad_df = rad_df.copy()
    path_df = path_df.copy()
    
    # Convert Patient_ID to string in both dataframes for consistent comparison
    rad_df['PATIENT_ID'] = rad_df['PATIENT_ID'].astype(str)
    path_df['PATIENT_ID'] = path_df['PATIENT_ID'].astype(str)
    
    # Convert date columns to datetime objects and rename to DATE
    rad_df['DATE'] = pd.to_datetime(rad_df['RADIOLOGY_DTM'])
    rad_df = rad_df.drop('RADIOLOGY_DTM', axis=1)
    
    path_df['DATE'] = pd.to_datetime(path_df['SPECIMEN_RECEIVED_DTM'])
    path_df = path_df.drop('SPECIMEN_RECEIVED_DTM', axis=1)
    
    return rad_df, path_df


def combine_dataframes(rad_df, path_df):
    """Combine radiology and pathology dataframes, keeping pathology on separate rows."""
    # Create pathology records with the same columns as rad_df
    path_rows = []
    for _, path_row in path_df.iterrows():
        # Create a new row with pathology data
        new_row = pd.Series(index=rad_df.columns)
        new_row['PATIENT_ID'] = path_row['PATIENT_ID']
        new_row['DATE'] = path_row['DATE']
        
        # Set pathology data
        new_row['Pathology_Laterality'] = path_row.get('Pathology_Laterality')
        new_row['final_diag'] = path_row.get('final_diag')
        new_row['path_interpretation'] = path_row.get('path_interpretation')
        
        path_rows.append(new_row)
    
    # Create a DataFrame with pathology records
    path_records_df = pd.DataFrame(path_rows)
    
    # Combine radiology records and pathology records
    final_df = pd.concat([rad_df, path_records_df], ignore_index=True)
    
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