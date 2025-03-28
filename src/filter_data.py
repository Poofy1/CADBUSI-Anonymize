import os
import pandas as pd

# Get the current script directory and go back one directory
env = os.path.dirname(os.path.abspath(__file__))
env = os.path.dirname(env)  # Go back one directory



def create_final_dataset(rad_df, path_df):
    print("Creating Final Dataset")
    
    # Create a copy of the original dataframe to avoid modifying it
    final_df = rad_df.copy()
    
    # Convert Patient_ID to string in both dataframes for consistent comparison
    final_df['PATIENT_ID'] = final_df['PATIENT_ID'].astype(str)
    path_df['PATIENT_ID'] = path_df['PATIENT_ID'].astype(str)
    
    # Convert date columns to datetime objects for comparison
    final_df['RADIOLOGY_DTM'] = pd.to_datetime(final_df['RADIOLOGY_DTM'])
    path_df['SPECIMEN_RECEIVED_DTM'] = pd.to_datetime(path_df['SPECIMEN_RECEIVED_DTM'])
    
    # Initialize the new columns we want to add from path_df
    final_df['Pathology_Laterality'] = None
    final_df['final_diag'] = None
    final_df['simple_diagnosis'] = None
    final_df['SPECIMEN_RECEIVED_DTM'] = None
    final_df['days_difference'] = None
    
    # Keep track of which pathology records have been matched
    matched_path_indices = set()
    
    # First pass - match pathology to closest radiology for each patient
    for patient_id in path_df['PATIENT_ID'].unique():
        # Get all radiology entries for this patient
        patient_rad_mask = final_df['PATIENT_ID'] == patient_id
        
        # Get all path entries for this patient
        patient_path_rows = path_df[path_df['PATIENT_ID'] == patient_id]
        
        if not any(patient_rad_mask):
            # No radiology records for this patient, will handle in second pass
            continue
            
        # For each pathology record
        for path_idx, path_row in patient_path_rows.iterrows():
            path_date = path_row['SPECIMEN_RECEIVED_DTM']
            
            # Calculate date differences properly using vectorized operations
            date_diffs = abs((path_date - final_df.loc[patient_rad_mask, 'RADIOLOGY_DTM']).dt.days)
            
            # Find the index with minimum difference
            if len(date_diffs) > 0:
                min_diff_idx = date_diffs.idxmin()
                date_diff = date_diffs[min_diff_idx]
                
                # Check if this rad record already has a closer pathology match
                current_diff = final_df.at[min_diff_idx, 'days_difference']
                
                if pd.isna(current_diff) or date_diff < current_diff:
                    # Update the radiology record with this pathology data
                    final_df.at[min_diff_idx, 'Pathology_Laterality'] = path_row.get('Pathology_Laterality')
                    final_df.at[min_diff_idx, 'final_diag'] = path_row.get('final_diag')
                    final_df.at[min_diff_idx, 'simple_diagnosis'] = path_row.get('simple_diagnosis')
                    final_df.at[min_diff_idx, 'SPECIMEN_RECEIVED_DTM'] = path_date
                    final_df.at[min_diff_idx, 'days_difference'] = int(date_diff)
                    matched_path_indices.add(path_idx)
    
    # Second pass - create new records for any unmatched pathology records
    new_rows = []
    for path_idx, path_row in path_df.iterrows():
        if path_idx in matched_path_indices:
            continue
        
        # Create a new row with just patient ID and pathology data
        new_row = pd.Series(index=final_df.columns)
        new_row['PATIENT_ID'] = path_row['PATIENT_ID']
        
        # Set pathology data
        new_row['Pathology_Laterality'] = path_row.get('Pathology_Laterality')
        new_row['final_diag'] = path_row.get('final_diag')
        new_row['simple_diagnosis'] = path_row.get('simple_diagnosis')
        new_row['SPECIMEN_RECEIVED_DTM'] = path_row['SPECIMEN_RECEIVED_DTM']
        new_row['days_difference'] = 0
        
        new_rows.append(new_row)
        matched_path_indices.add(path_idx)
    
    # Append the new rows
    if new_rows:
        new_df = pd.DataFrame(new_rows)
        final_df = pd.concat([final_df, new_df], ignore_index=True)
    
    # Save to CSV
    final_df.to_csv(f'{env}/raw_data/final_dataset.csv', index=False)
    
    print(f"Final dataset created with {len(final_df)} records")
    print(f"Records with matched pathology data: {final_df['SPECIMEN_RECEIVED_DTM'].notna().sum()}")
    print(f"Original pathology records used: {len(matched_path_indices)} out of {len(path_df)}")
    print(f"Synthetic records created: {len(new_rows)}")
    
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
        
        # Call the filter function
        create_final_dataset(rad_df, path_df)
        
    except FileNotFoundError as e:
        print(f"Error: {e}")
        print("Please make sure you've run the parsing scripts to create the parsed CSV files first.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

# Run the main function when the script is executed directly
if __name__ == "__main__":
    main()