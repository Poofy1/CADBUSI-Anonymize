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

"""
def process_single_dcm_file(dicom_file, target_directory, encryption_key, save_png, png_directory):
    # Read the DICOM file
    dataset = pydicom.dcmread(dicom_file, force=True)

    # Check media type and additional conditions
    media_type = dicom_media_type(dataset)
    if (media_type == 'image' and (0x0018, 0x6011) in dataset) or media_type == 'multi':
        
        # Create a new filename using encryption
        new_filename, dataset = create_dcm_filename(dataset, encryption_key)
        
        # De-identify the DICOM dataset
        dataset = deidentify_dicom(dataset)
        
        # Set the target path to write the DICOM file
        target_path = os.path.join(target_directory, new_filename)

        # Make sure target directory exists
        if not os.path.exists(os.path.dirname(target_path)):
            os.makedirs(os.path.dirname(target_path))
        
        # Write the DICOM dataset to a new DICOM file
        dataset.save_as(target_path)

        # If save_png flag is True, save the image data as a PNG file
        if save_png:
            # Convert the Pixel Array data to a PIL Image object
            image = Image.fromarray(dataset.pixel_array)

            # Save the Image object as a PNG file
            png_file_path = os.path.join(png_directory, new_filename.replace('.dcm', '.png'))
            image.save(png_file_path, "PNG")
    
    return dicom_file

def deidentify_dcm_files(directory_path, unzipped_path, target_directory, encryption_key, save_png=False):
    os.makedirs(target_directory, exist_ok=True)
    
    if save_png:
        png_directory = os.path.join(directory_path, 'png_debug')
        os.makedirs(png_directory, exist_ok=True)
    else:
        png_directory = None
    
    # First, collect all the DICOM file paths
    dicom_files = []
    for root, dirs, files in os.walk(unzipped_path):
        for file in files:
            if file.lower().endswith(".dcm"):
                dicom_files.append(os.path.join(root, file))

    # Use ThreadPoolExecutor to process DICOM files in parallel
    with ThreadPoolExecutor() as executor:
        # Submit tasks to the executor
        futures = {executor.submit(process_single_dcm_file, dicom_file, target_directory, encryption_key, 
                                 save_png, png_directory): dicom_file for dicom_file in dicom_files}
        
        # As each future is completed, retrieve the result
        for future in tqdm(as_completed(futures), total=len(futures), desc="Processing DICOM files"):
            try:
                # Get the result of the future
                result = future.result()
            except Exception as exc:
                print(f'An exception occurred: {exc}')

"""


def process_single_blob(blob, client, output_bucket_name, output_bucket_path, encryption_key, save_png):
    """Process a single DICOM blob from GCP bucket"""
    try:
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
                
                # Set the target path in GCP
                output_blob_path = os.path.join(output_bucket_path, new_filename)
                
                # Save the deidentified DICOM to a temporary file
                temp_output_path = f"{temp_file.name}_output"
                dataset.save_as(temp_output_path)
                
                # Upload the deidentified DICOM back to GCP
                output_bucket = client.bucket(output_bucket_name)
                output_blob = output_bucket.blob(output_blob_path)
                output_blob.upload_from_filename(temp_output_path)
                
                # If save_png flag is True, save as PNG and upload to GCP
                if save_png:
                    # Convert the Pixel Array data to a PNG
                    image = Image.fromarray(dataset.pixel_array)
                    
                    # Save to a temporary file
                    temp_png_path = f"{temp_file.name}.png"
                    image.save(temp_png_path, "PNG")
                    
                    # Upload PNG to GCP
                    png_output_path = output_blob_path.replace('.dcm', '.png')
                    png_blob = output_bucket.blob(png_output_path)
                    png_blob.upload_from_filename(temp_png_path)
                    
                    # Remove temporary PNG file
                    os.unlink(temp_png_path)
                
                # Remove temporary output file
                os.unlink(temp_output_path)
                
        # Remove the temporary input file
        os.unlink(temp_file.name)
        
        return blob.name
        
    except Exception as e:
        print(f"Error processing {blob.name}: {e}")
        return None


def deidentify_bucket_dicoms(bucket_name, bucket_path, output_bucket_name, output_bucket_path, encryption_key, save_png=False):
    """Process DICOM files from a GCP bucket and upload deidentified versions to output bucket"""
    # Initialize storage client
    client = storage.Client()
    
    # Get the bucket and list all blobs matching the path
    bucket = client.bucket(bucket_name)
    blobs = list(bucket.list_blobs(prefix=bucket_path))
    
    # Filter for only DICOM files
    dicom_blobs = [blob for blob in blobs if blob.name.lower().endswith('.dcm')]
    
    print(f"Found {len(dicom_blobs)} DICOM files in the bucket path")
    
    # Use ThreadPoolExecutor to process blobs in parallel
    with ThreadPoolExecutor() as executor:
        # Submit tasks to the executor
        futures = {
            executor.submit(
                process_single_blob, 
                blob, 
                client, 
                output_bucket_name, 
                output_bucket_path, 
                encryption_key, 
                save_png
            ): blob for blob in dicom_blobs
        }
        
        # As each future is completed, retrieve the result
        for future in tqdm(as_completed(futures), total=len(futures), desc="Processing DICOM files"):
            try:
                result = future.result()
            except Exception as exc:
                print(f'An exception occurred: {exc}')


# Example usage
if __name__ == "__main__":
    BUCKET_NAME = "shared-aif-bucket-87d1"
    BUCKET_PATH = "Downloads"
    BUCKET_OUTPUT_NAME = BUCKET_NAME 
    BUCKET_OUTPUT_PATH = "anon_dicoms"
    
    # Get encryption key from the environment or configuration
    encryption_key = get_encryption_key()  # This function should be defined in src.encrypt_keys
    
    # Process the DICOM files
    deidentify_bucket_dicoms(
        bucket_name=BUCKET_NAME,
        bucket_path=BUCKET_PATH,
        output_bucket_name=BUCKET_OUTPUT_NAME,
        output_bucket_path=BUCKET_OUTPUT_PATH,
        encryption_key=encryption_key,
        save_png=False  # Set to True if you want to save PNGs as well
    )