import os
import pandas as pd

# Get the current script directory and go back one directory
env = os.path.dirname(os.path.abspath(__file__))
env = os.path.dirname(env)  # Go back one directory


def determine_final_interpretation(final_df):
    """Determine final_interpretation for each patient based on specified rules."""
    # Get today's date to check for sufficient follow-up period
    today = pd.Timestamp.now()
    
    # Define trigger words that indicate possible malignancy
    trigger_words = ['malignant', 'cancer', 'carcinoma', 'intermediate', 'malignancy']
    
    for patient_id in final_df['PATIENT_ID'].unique():
        # Get all records for this patient
        patient_mask = final_df['PATIENT_ID'] == patient_id
        patient_records = final_df[patient_mask].copy()
        
        # Sort by date for chronological processing
        patient_records = patient_records.sort_values('DATE')
        
        # Process each row chronologically
        for idx, row in patient_records.iterrows():
            # Skip rows with missing effective date
            if pd.isna(row['DATE']):
                continue
                
            # Check if this is an ultrasound
            if row['MODALITY'] == 'US':
                # Only consider this row if it's old enough to have 18 months of follow-up data
                if (today - row['DATE']).days < 540:  # Less than 18 months of follow-up
                    continue
                    
                # Define the window: 3 months back, 18 months forward
                start_date = row['DATE'] - pd.Timedelta(days=90)
                end_date = row['DATE'] + pd.Timedelta(days=540)
                
                # Find records in the specified time window
                records_in_timeframe = patient_records[
                    (patient_records['DATE'] >= start_date) &
                    (patient_records['DATE'] <= end_date)
                ]
                
                # Check for any trigger words in Biopsy or simple_diagnosis fields
                has_trigger_words = False
                for _, record in records_in_timeframe.iterrows():
                    # Check Biopsy field if it exists and is not None/NaN
                    if 'Biopsy' in record and pd.notna(record['Biopsy']):
                        biopsy_text = str(record['Biopsy']).lower()
                        if any(word in biopsy_text for word in trigger_words):
                            has_trigger_words = True
                            break
                    
                    # Check simple_diagnosis field if it exists and is not None/NaN
                    if 'simple_diagnosis' in record and pd.notna(record['simple_diagnosis']):
                        diagnosis_text = str(record['simple_diagnosis']).lower()
                        if any(word in diagnosis_text for word in trigger_words):
                            has_trigger_words = True
                            break
                
                # If no trigger words found, mark as BENIGN
                if not has_trigger_words:
                    final_df.at[idx, 'final_interpretation'] = 'BENIGN'
    
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
        new_row['simple_diagnosis'] = path_row.get('simple_diagnosis')
        
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
    final_df.to_csv(f'{env}/raw_data/final_dataset.csv', index=False)
    
    # Print statistics
    print(f"Final dataset created with {len(final_df)} records")
    print(f"Original radiology records: {len(rad_df)}")
    print(f"Added pathology records: {len(path_df)}")
    print(f"Records marked as BENIGN: {(final_df['final_interpretation'] == 'BENIGN').sum()}")
    print(f"Records marked as MALIGNANT: {(final_df['final_interpretation'] == 'MALIGNANT').sum()}")
    
    return final_df


# Main function to run the entire process
def main():
    # Load the parsed radiology and pathology data
    try:
        rad_file_path = f'{env}/raw_data/parsed_radiology.csv'
        path_file_path = f'{env}/raw_data/parsed_pathology.csv'
        
        print(f"Loading radiology data from: {rad_file_path}")
        rad_df = pd.read_csv(rad_file_path)
        print(f"Loaded radiology data with {len(rad_df)} records")
        
        print(f"Loading pathology data from: {path_file_path}")
        path_df = pd.read_csv(path_file_path)
        print(f"Loaded pathology data with {len(path_df)} records")
        
        # Call the create_final_dataset function
        create_final_dataset(rad_df, path_df)
        
    except FileNotFoundError as e:
        print(f"Error: {e}")
        print("Please make sure you've run the parsing scripts to create the parsed CSV files first.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")


# Run the main function when the script is executed directly
if __name__ == "__main__":
    main()