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
    path_df['SPECIMEN_ACCESSION_DTM'] = pd.to_datetime(path_df['SPECIMEN_ACCESSION_DTM'])
    
    # Initialize the new columns we want to add from path_df
    final_df['Pathology_Laterality'] = None
    final_df['final_diag'] = None
    final_df['simple_diagnosis'] = None
    final_df['path_date'] = None
    final_df['days_difference'] = None
    
    # Define the maximum time difference (90 days = 3 months)
    MAX_DAYS_DIFF = 90
    
    # For each row in the radiology dataframe
    for idx, rad_row in final_df.iterrows():
        patient_id = rad_row['PATIENT_ID']
        rad_date = rad_row['RADIOLOGY_DTM']
        
        # Get all path entries for this patient
        patient_path_rows = path_df[path_df['PATIENT_ID'] == patient_id]
        
        if not patient_path_rows.empty:
            # Filter to keep only path dates after the rad date
            future_path_rows = patient_path_rows[patient_path_rows['SPECIMEN_ACCESSION_DTM'] > rad_date]
            
            if not future_path_rows.empty:
                # Find the closest date
                closest_path_row = future_path_rows.loc[future_path_rows['SPECIMEN_ACCESSION_DTM'].idxmin()]
                
                # Calculate date difference in days
                date_diff = (closest_path_row['SPECIMEN_ACCESSION_DTM'] - rad_date).days
                
                # Only match if within 3 months
                if date_diff <= MAX_DAYS_DIFF:
                    # Add path data to the final dataframe
                    final_df.at[idx, 'Pathology_Laterality'] = closest_path_row['Pathology_Laterality']
                    final_df.at[idx, 'final_diag'] = closest_path_row['final_diag']
                    final_df.at[idx, 'simple_diagnosis'] = closest_path_row['simple_diagnosis']
                    final_df.at[idx, 'SPECIMEN_ACCESSION_DTM'] = closest_path_row['SPECIMEN_ACCESSION_DTM']
                    final_df.at[idx, 'days_difference'] = date_diff
    
    # Save to CSV
    final_df.to_csv(f'{env}/raw_data/final_dataset.csv', index=False)
    
    print(f"Final dataset created with {len(final_df)} records")
    print(f"Records with matched pathology data: {final_df['SPECIMEN_ACCESSION_DTM'].notna().sum()} (within 3 months)")
    
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