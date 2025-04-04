from src.dicom_download import *
from src.query import *
from src.anonymize_dicoms import *
from src.encrypt_keys import *
from src.query_clean_path import filter_path_data
from src.query_clean_rad import filter_rad_data
from src.filter_data import create_final_dataset
import argparse
import os
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
    parser.add_argument('--anon', type=str, help='Directory name for anonymized DICOM output')
    
    return parser.parse_args()

def main():
    """Main entry point for the script."""
    args = parse_arguments()
    
    
    dicom_query_file = f'{env}/output/endpoint_data.csv'
    anon_file = f'{env}/output/anon_data.csv'
    
    
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
        rad_df = filter_rad_data(rad_df)
        path_df = filter_path_data(path_df)
        
        # Filter data
        create_final_dataset(rad_df, path_df)
    
    elif args.deploy or args.cleanup or args.rerun:
        dicom_download_remote_start(dicom_query_file, args.deploy, args.cleanup)
        
    elif args.anon:

        
        key_output = f'{env}/encryption_key.pkl'
        key = encrypt_ids(dicom_query_file, anon_file, key_output)
        

        BUCKET_NAME = "shared-aif-bucket-87d1"
        BUCKET_PATH = f"Downloads/{args.anon}"
        BUCKET_OUTPUT_NAME = BUCKET_NAME 
        BUCKET_OUTPUT_PATH = f"anon_dicoms/{args.anon}"
        deidentify_bucket_dicoms(
            bucket_name=BUCKET_NAME,
            bucket_path=BUCKET_PATH,
            output_bucket_name=BUCKET_OUTPUT_NAME,
            output_bucket_path=BUCKET_OUTPUT_PATH,
            encryption_key=key
        )
    
    else:
        print("No action specified. Use --help for available options.")

if __name__ == "__main__":
    main()