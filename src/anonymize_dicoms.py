import pydicom
import os
from PIL import Image
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from PIL import Image
import hashlib
from src.encrypt_keys import *
from google.cloud import storage
import tempfile
env = os.path.dirname(os.path.abspath(__file__))


# Add parent directory to path
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import CONFIG

def anon_callback(ds, element):
    names_to_remove = [
        'SOP Instance UID',
        'Study Time',
        'Series Time',
        'Content Time',
        'Study Instance UID',
        'Series Instance UID',
        'Private Creator',
        'Media Storage SOP Instance UID',
        'Implementation Class UID',
        "Patient's Name",
        "Referring Physician's Name",
        "Acquisition DateTime",
        "Institution Name",
        "Station Name",
        "Physician(s) of Record",
        "Referenced SOP Class UID",
        "Referenced SOP Instance UID",
        "Device Serial Number",
        "Patient Comments",
        "Issuer of Patient ID",
        "Study ID",
        "Study Comments",
        "Current Patient Location",
        "Requested Procedure ID",
        "Performed Procedure Step ID",
        "Other Patient IDs",
        "Operators' Name",
        "Institutional Department Name",
        "Manufacturer",
        "Requesting Physician",
    ]
    
    names_to_anon_time = [
        'Study Time',
        'Series Time',
        'Content Time',
    ]
    
    if element.tag in ds:  # Check if the tag exists before attempting deletion
        if element.name in names_to_remove:
            del ds[element.tag]

    if element.VR == "DA":
        date = element.value
        date = date[0:4] + "0101"  # set all dates to YYYY0101
        element.value = date

    if element.VR == "TM" and element.name not in names_to_anon_time:
        element.value = "000000"  # set time to zeros
    

def dicom_media_type(dataset):
    if hasattr(dataset, 'file_meta') and (0x00020002) in dataset.file_meta:
        type = str(dataset.file_meta[0x00020002].value)
        if type == '1.2.840.10008.5.1.4.1.1.6.1':  # single ultrasound image
            return 'image'
        elif type == '1.2.840.10008.5.1.4.1.1.3.1':  # multi-frame ultrasound image
            return 'multi'
        else:
            return 'other'  # something else
    else:
        return 'unknown'


def deidentify_dicom(ds):
    ds.remove_private_tags() # take out private tags added by notion or otherwise

    ds.file_meta.walk(anon_callback)
    ds.walk(anon_callback)

    media_type = ds.file_meta[0x00020002]
    is_video = str(media_type).find('Multi-frame') > -1
    is_secondary = str(media_type).find('Secondary') > -1
    if is_secondary:
        y0 = 101
    else:
        if (0x0018, 0x6011) in ds:
            y0 = ds['SequenceOfUltrasoundRegions'][0]['RegionLocationMinY0'].value
        else:
            y0 = 101

    if 'OriginalAttributesSequence' in ds:
        del ds.OriginalAttributesSequence
        
    # Check if Pixel Data is compressed
    if ds.file_meta.TransferSyntaxUID.is_compressed:
        # Attempt to decompress the Pixel Data
        try:
            ds.decompress()
        except NotImplementedError as e:
            print(f"Decompression not implemented for this transfer syntax: {e}")
            return None  # or handle this appropriately for your use case
        except Exception as e:
            print(f"An error occurred during decompression: {e}")
            return None  # or handle this appropriately for your use case

    # crop patient info above US region 
    arr = ds.pixel_array
    
    if is_video:
        arr[:,:y0] = 0
    else:
        arr[:y0] = 0
    
    # Update the Pixel Data
    ds.PixelData = arr.tobytes()
    
    ds.file_meta.TransferSyntaxUID = ds.file_meta.TransferSyntaxUID

    return ds


def create_dcm_filename(ds, key):
        
    # Extract the necessary identifiers
    accession_number = ds.AccessionNumber
    patient_id = ds.PatientID
    
    
    
    # Encrypt identifiers using the new method
    anonymized_patient_id = encrypt_single_id(key, patient_id)
    anonymized_accession_number = encrypt_single_id(key, accession_number)
    
    # Check the media type
    media_type = ds.file_meta[0x00020002]
    is_video = str(media_type).find('Multi-frame') > -1
    is_secondary = str(media_type).find('Secondary') > -1

    if is_video:
        media = 'video'
    elif is_secondary:
        media = 'second'
    else:
        media = 'image'
    
    # Create a hash object
    hash_obj = hashlib.sha256()
    hash_obj.update(ds.pixel_array.tobytes())  # Convert pixel_array to bytes before hashing
    
    image_hash = hash_obj.hexdigest()
    
    # Try to convert encrypted IDs to integers and pad to 8 digits
    try:
        anon_patient_id_int = int(anonymized_patient_id)
        formatted_patient_id = f"{anon_patient_id_int:08}"
    except ValueError:
        formatted_patient_id = anonymized_patient_id
        
    try:
        anon_accession_number_int = int(anonymized_accession_number)
        formatted_accession_number = f"{anon_accession_number_int:08}"
    except ValueError:
        formatted_accession_number = anonymized_accession_number
    
    # Construct the filename using the anonymized identifiers
    filename = f'{media}_{formatted_patient_id}_{formatted_accession_number}_{image_hash}.dcm'

    # Anonymize the DICOM data - set the new IDs
    ds.PatientID = anonymized_patient_id
    ds.AccessionNumber = anonymized_accession_number

    return filename, ds  # return the modified DICOM dataset along with the filename


def process_single_blob(blob, client, output_bucket_name, output_bucket_path, encryption_key):
    """Process a single DICOM blob from GCP bucket"""
    try:
        # Extract study_id from the original path
        path_parts = blob.name.split('/')
        # Assuming the path structure is {bucket_path}/{date}/{study_id}/...dicoms
        study_id = path_parts[2] if len(path_parts) > 2 else "unknown_study"
        
        # Download blob to a temporary file
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            blob.download_to_filename(temp_file.name)
            
            # Read the DICOM data
            dataset = pydicom.dcmread(temp_file.name, force=True)
            
            # Check if this is a DICOM file we want to process
            media_type = dicom_media_type(dataset)
            if (media_type == 'image' and (0x0018, 0x6011) in dataset) or media_type == 'multi':
                
                # Create a new filename using encryption
                new_filename, dataset = create_dcm_filename(dataset, encryption_key)
                
                # De-identify the DICOM dataset
                dataset = deidentify_dicom(dataset)
                
                # Create folder structure based on PatientID_AccessionNumber
                folder_name = f"{dataset.PatientID}_{dataset.AccessionNumber}"
                
                # Set the target path in GCP - now including study_id
                output_blob_path = os.path.join(output_bucket_path, folder_name, study_id, new_filename)
                
                # Save the deidentified DICOM to a temporary file
                temp_output_path = f"{temp_file.name}_output"
                dataset.save_as(temp_output_path)
                
                # Upload the deidentified DICOM back to GCP
                output_bucket = client.bucket(output_bucket_name)
                output_blob = output_bucket.blob(output_blob_path)
                output_blob.upload_from_filename(temp_output_path)
                
                # Remove temporary output file
                os.unlink(temp_output_path)
                
        # Remove the temporary input file
        os.unlink(temp_file.name)
        
        return blob.name
        
    except Exception as e:
        print(f"Error processing {blob.name}: {e}")
        return None

def deidentify_bucket_dicoms(bucket_path, output_bucket_path, encryption_key, batch_size=100):
    """Process DICOM files from a GCP bucket and upload deidentified versions to output bucket"""
    # Initialize storage client
    client = storage.Client()
    
    # Get the bucket
    bucket = client.bucket(CONFIG["storage"]["bucket_name"])
    
    # Process in batches to avoid memory issues
    blobs_iterator = bucket.list_blobs(prefix=bucket_path)
    
    total_processed = 0
    successful = 0
    failed = 0
    
    # Process in batches
    current_batch = []
    
    print(f"Starting batch processing of DICOM files...")
    
    for blob in blobs_iterator:
        if not blob.name.lower().endswith('.dcm'):
            continue
            
        current_batch.append(blob)
        
        # Process when batch is full
        if len(current_batch) >= batch_size:
            success, fail = process_batch(current_batch, client, CONFIG["storage"]["bucket_name"], 
                                         output_bucket_path, encryption_key)
            successful += success
            failed += fail
            total_processed += len(current_batch)
            print(f"Processed {total_processed} files. Success: {successful}, Failed: {failed}")
            current_batch = []
    
    # Process any remaining files
    if current_batch:
        success, fail = process_batch(current_batch, client, CONFIG["storage"]["bucket_name"], 
                                     output_bucket_path, encryption_key)
        successful += success
        failed += fail
        total_processed += len(current_batch)
        
    print(f"Processing complete. Total: {total_processed}, Success: {successful}, Failed: {failed}")
    return successful, failed

def process_batch(blob_batch, client, output_bucket_name, output_bucket_path, encryption_key):
    """Process a batch of DICOM blobs"""
    successful = 0
    failed = 0
    
    # Use ThreadPoolExecutor with a limited number of workers
    with ThreadPoolExecutor(max_workers=10) as executor:
        # Submit tasks to the executor
        futures = {
            executor.submit(
                process_single_blob, 
                blob, 
                client, 
                output_bucket_name, 
                output_bucket_path, 
                encryption_key
            ): blob for blob in blob_batch
        }
        
        # Process results as they complete
        for future in tqdm(as_completed(futures), total=len(futures), desc="Processing batch"):
            try:
                result = future.result()
                if result:
                    successful += 1
                else:
                    failed += 1
            except Exception as exc:
                print(f'An exception occurred: {exc}')
                failed += 1
                
    return successful, failed