from src.dicom_download import *
from src.query import *
from src.anonymize_dicoms import *
from src.encrypt_keys import *
from src.query_clean_path import filter_path_data
from src.query_clean_rad import filter_rad_data
from src.filter_data import create_final_dataset
from config import CONFIG
import argparse
import os
import sys
env = os.path.dirname(os.path.abspath(__file__))



def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='DICOM processing pipeline')
    
    # Query arguments
    parser.add_argument('--query', action='store_true', help='Run breast imaging query')
    parser.add_argument('--limit', type=int, help='Optional limit for the query (e.g., 10)')
    
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
    key_output = f'{env}/encryption_key.pkl'
    output_path = os.path.join(env, "raw_data")
    
    # Handle query command
    if args.query:
        limit = args.limit
        
        # If no limit is specified, ask for confirmation
        if limit is None:
            confirmation = input("No limit specified. Are you sure you want to query without a limit? (y/n): ")
            if confirmation.lower() not in ['y', 'yes']:
                print("Query cancelled.")
                sys.exit(0)
            print("Proceeding with unlimited query.")
        else:
            print(f"Setting query limit to {limit}")
        
        # Run the query with the specified limit
        rad_df, path_df = run_breast_imaging_query(limit=limit)

        # Parse that data
        rad_df = filter_rad_data(rad_df, output_path)
        path_df = filter_path_data(path_df, output_path)
        
        # Filter data
        create_final_dataset(rad_df, path_df, output_path)
    
    elif args.deploy or args.cleanup or args.rerun:
        dicom_download_remote_start(dicom_query_file, args.deploy, args.cleanup)
        
    elif args.anon:
        
        anon_file_gcp = f'{CONFIG["storage"]["anonymized_path"]}/{args.anon}/anon_data.csv'
        anon_file_local = f'{env}/output/anon_data.csv'
        
        key = encrypt_ids(dicom_query_file, anon_file_gcp, anon_file_local, key_output)
        
        BUCKET_PATH = f'{CONFIG["storage"]["download_path"]}/{args.anon}'
        BUCKET_OUTPUT_PATH = f'{CONFIG["storage"]["anonymized_path"]}/{args.anon}'
        deidentify_bucket_dicoms(
            bucket_path=BUCKET_PATH,
            output_bucket_path=BUCKET_OUTPUT_PATH,
            encryption_key=key
        )
    
    else:
        print("No action specified. Use --help for available options.")

if __name__ == "__main__":
    main()