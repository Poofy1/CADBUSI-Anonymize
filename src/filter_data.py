import os
import pandas as pd
import re

# Get the current script directory and go back one directory
env = os.path.dirname(os.path.abspath(__file__))
env = os.path.dirname(env)  # Go back one directory

def filter_path_data(rad_df, path_df):
    print("Creating Final Dataset")
    
    # Create a copy of the original dataframe to avoid modifying it
    final_df = rad_df.copy()
    
    # Convert Patient_ID to string in both dataframes for consistent comparison
    final_df['PATIENT_ID'] = final_df['PATIENT_ID'].astype(str)
    path_df['PATIENT_ID'] = path_df['PATIENT_ID'].astype(str)
    
    # Get unique patient IDs from pathology dataset
    path_patient_ids = set(path_df['PATIENT_ID'].unique())
    
    # Filter for US modality (if available in the dataset)
    if 'MODALITY' in final_df.columns:
        final_df = final_df[final_df['MODALITY'] == 'US']
    elif 'Modality' in final_df.columns:
        final_df = final_df[final_df['Modality'] == 'US']
    
    # Filter for patients that don't exist in pathology dataset
    final_df = final_df[~final_df['PATIENT_ID'].isin(path_patient_ids)]
    
    # Save to CSV
    final_df.to_csv(f'{env}/raw_data/final_dataset.csv', index=False)
    
    print(f"Final dataset created with {len(final_df)} records")
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
        final_df = filter_path_data(rad_df, path_df)
        
    except FileNotFoundError as e:
        print(f"Error: {e}")
        print("Please make sure you've run the parsing scripts to create the parsed CSV files first.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

# Run the main function when the script is executed directly
if __name__ == "__main__":
    main()