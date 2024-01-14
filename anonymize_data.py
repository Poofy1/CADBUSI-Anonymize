import pydicom
import os
import zipfile
from PIL import Image
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from PIL import Image
import pandas as pd
import hashlib
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
    



def dicom_media_type( dataset ):
    type = str( dataset.file_meta[0x00020002].value )
    if type == '1.2.840.10008.5.1.4.1.1.6.1': # single ultrasound image
        return 'image'
    elif type == '1.2.840.10008.5.1.4.1.1.3.1': # multi-frame ultrasound image
        return 'multi'
    else:
        return 'other' # something else

def extract_single_zip_file(file_name, output_folder):
    if os.path.exists(output_folder):
        return

    try:
        zip_ref = zipfile.ZipFile(file_name)  # create zipfile object
        zip_ref.extractall(output_folder)  # extract file to dir
        zip_ref.close()  # close file
    except Exception as e:
        print(f'Skipping Bad Zip File: {file_name}. Exception: {e}')

def unzip_files_in_directory(directory_path, target_dir):
    os.makedirs(target_dir, exist_ok=True)

    if len(os.listdir(directory_path)) == 0:
        print("No zip files found")
        return

    print("Unzipping Files")

    with ThreadPoolExecutor() as executor:
        futures = [
            executor.submit(
                extract_single_zip_file, 
                os.path.join(directory_path, item), 
                os.path.join(target_dir, os.path.splitext(item)[0])
            ) 
            for item in os.listdir(directory_path) 
            if item.endswith('.zip')
        ]

        for future in tqdm(as_completed(futures), total=len(futures), desc=""):
            try:
                future.result()
            except Exception as exc:
                print(f'An exception occurred: {exc}')


def deidentify_dicom( ds ):
    ds.remove_private_tags() # take out private tags added by notion or otherwise

    ds.file_meta.walk(anon_callback)
    ds.walk(anon_callback)

    media_type = ds.file_meta[0x00020002]
    is_video = str(media_type).find('Multi-frame')>-1
    is_secondary = str(media_type).find('Secondary')>-1
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




def create_dcm_filename(ds, master_anon_map):
    # Extract the necessary identifiers
    accession_number = ds.AccessionNumber

    # Find the anonymized values
    anonymized_patient_id = master_anon_map.loc[master_anon_map[' OriginalAccessionNumber'] == accession_number, ' AnonymizedPatientID'].values[0]
    anonymized_accession_number = master_anon_map.loc[master_anon_map[' OriginalAccessionNumber'] == accession_number, ' AnonymizedAccessionNumber'].values[0]

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
    
    # Construct the filename using the anonymized identifiers
    filename = f'{media}_{anonymized_patient_id:08}_{anonymized_accession_number:08}_{image_hash}.dcm'

    # Anonymize the DICOM data
    ds.PatientID = str(anonymized_patient_id).rjust(8, '0')  # assuming the anonymized ID is an integer
    ds.AccessionNumber = str(anonymized_accession_number).rjust(8, '0')

    return filename, ds  # return the modified DICOM dataset along with the filename



def process_single_dcm_file(dicom_file, target_directory, df_master_anon_map, save_png, png_directory):
    # Read the DICOM file
    dataset = pydicom.dcmread(dicom_file, force=True)

    # Check media type and additional conditions
    media_type = dicom_media_type(dataset)
    if (media_type == 'image' and (0x0018, 0x6011) in dataset) or media_type == 'multi':
        
        # Create a new filename
        new_filename, dataset = create_dcm_filename(dataset, df_master_anon_map)
        
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

    
    
    
def deidentify_dcm_files(directory_path, unzipped_path, target_directory, save_png=False):
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
    
    # Load the mapping files
    df_master_anon_map = pd.read_csv(f"{env}/maps/master_anon_map.csv")
    df_master_anon_map[' OriginalAccessionNumber'] = df_master_anon_map[' OriginalAccessionNumber'].astype(str)

    # Use ThreadPoolExecutor to process DICOM files in parallel
    with ThreadPoolExecutor() as executor:
        # Submit tasks to the executor
        futures = {executor.submit(process_single_dcm_file, dicom_file, target_directory, df_master_anon_map, save_png, png_directory): dicom_file for dicom_file in dicom_files}
        
        # As each future is completed, retrieve the result
        for future in tqdm(as_completed(futures), total=len(futures), desc="Processing DICOM files"):
            try:
                # Get the result of the future
                result = future.result()
            except Exception as exc:
                print(f'An exception occurred: {exc}')





data_dir = 'D:/DATA/CASBUSI/'
zipped_dicom_path = f'{data_dir}/zip_files'
unzipped_dicom_path = f'{data_dir}/unzipped_dicoms'
deidentified_path = f'{data_dir}/anonymized'

# Unzip everything
unzip_files_in_directory(zipped_dicom_path, unzipped_dicom_path)

# Deidentify everything
deidentify_dcm_files(data_dir, unzipped_dicom_path, deidentified_path, save_png=False) # save_png is for debugging
