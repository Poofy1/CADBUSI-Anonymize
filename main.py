from src.dicom_download import *
from src.query import *
from src.anonymize_dicoms import *
from src.encrypt_keys import *
from src.query_clean_path import filter_path_data
from src.query_clean_rad import filter_rad_data
import argparse
import os
import time
import sys
env = os.path.dirname(os.path.abspath(__file__))



def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='DICOM processing pipeline')
    
    # Query arguments
    parser.add_argument('--query', help='Run query with optional limit parameter (e.g., limit=10)')
    
    # Download arguments
    parser.add_argument('--deploy', action='store_true', help='Deploy FastAPI to Cloud Run')
    parser.add_argument('--rerun', action='store_true', help='Send message to pre-deployed FastAPI on Cloud Run')
    parser.add_argument('--cleanup', action='store_true', help='Clean up resources')
    
    # Anonymize arguments
    parser.add_argument('--anon', action='store_true', help='Path to directory with DICOMs to anonymize')
    
    return parser.parse_args()

def main():
    """Main entry point for the script."""
    args = parse_arguments()
    
    
    dicom_query_file = f'{env}/dicom_query.csv'
    
    
    # Handle query command
    if args.query is not None:
        limit = None
        # Parse the limit parameter if provided
        if args.query.startswith('limit='):
            try:
                limit = int(args.query.split('=')[1])
                print(f"Setting query limit to {limit}")
            except ValueError:
                print(f"Invalid limit value: {args.query.split('=')[1]}")
                sys.exit(1)
        
        # Run the query with the specified limit
        rad_df, path_df = run_breast_imaging_query(limit=limit)

        # Parse that data
        filter_rad_data(rad_df)
        filter_path_data(path_df)
    
    elif args.deploy or args.cleanup or args.rerun:
        dicom_download_remote_start(dicom_query_file, args.deploy, args.cleanup)
    elif args.anon:

        
        output_file = f'{env}/encrypted_output.csv'
        key_output = f'{env}/encryption_key.pkl'
        key = encrypt_ids(dicom_query_file, output_file, key_output)
        
        input_dicom_path = f'{env}/dicoms'
        deidentified_path = f'{env}/anonymized'
        deidentify_dcm_files(env, input_dicom_path, deidentified_path, key, save_png=True)
    
    else:
        print("No action specified. Use --help for available options.")

if __name__ == "__main__":
    main()