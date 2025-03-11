from src.dicom_download import *
from src.query import *
from src.anonymize_dicoms import *
from src.encrypt_keys import *
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
    parser.add_argument('--csv', help='Path to CSV file with DICOM URLs for downloading')
    parser.add_argument('--deploy', action='store_true', help='Deploy FastAPI to Cloud Run')
    parser.add_argument('--cleanup', action='store_true', help='Clean up resources')
    
    # Anonymize arguments
    parser.add_argument('--anonymize', help='Path to directory with DICOMs to anonymize')
    
    return parser.parse_args()

def main():
    """Main entry point for the script."""
    args = parse_arguments()
    
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
        df = run_breast_imaging_query(limit=limit)
        
        # Save results to CSV
        output_file = 'dicom_urls.csv'
        print(f"\nSaving results to {output_file}...")
        df.to_csv(output_file, index=False)
        print(f"Results successfully saved to {output_file}")
        
        
        # Next process / simplify that data (wip)
    
    elif args.download:
        dicom_download_remote_start(args.csv, args.deploy, args.cleanup)
    elif args.anonymize:

        input_file = f'{env}/dicom_urls.csv'
        output_file = f'{env}/encrypted_output.csv'
        key_output = f'{env}/encryption_key.pkl'
        key = encrypt_ids(input_file, output_file, key_output)
        
        input_dicom_path = f'{env}/dicoms'
        deidentified_path = f'{env}/anonymized'
        deidentify_dcm_files(data_dir, input_dicom_path, deidentified_path, key, save_png=True)
    
    else:
        print("No action specified. Use --help for available options.")
        sys.exit(1)

if __name__ == "__main__":
    main()