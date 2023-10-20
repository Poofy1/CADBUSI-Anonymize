import matplotlib.pyplot as plt
from pydicom import dcmread
from hashlib import sha1
import pydicom
import os
import zipfile
import csv
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
from tqdm import tqdm
import numpy as np



def anon_callback(ds, element):
    """used with "walk" to loop over dicom and anonymize entries

    Args:

    Returns:

    """
    names = ['SOP Instance UID','Study Time','Series Time','Content Time',
             'Study Instance UID','Series Instance UID','Private Creator',
             'Media Storage SOP Instance UID',
             'Implementation Class UID']
    
    if element.name in names:
        element.value = "anon"

    if element.VR == "DA":
        date = element.value
        date = date[0:4] + "0101" # set all dates to YYYY0101
        element.value = date

    if element.VR == "TM":
        element.value = "anon"

def deidentify_dicom_dataset( ds ):
    """remove patient information from pydicom dataset that Notion has partially deanonymized

    Args:
        ds: pydicom dataset from reading dicom file with pydicom

    Returns:
        out_ds: pydicom dataset with private information removed from header and image
        is_video:  True for Multi-frame dicom, False for single image
        hash: sha1 hash of str(ds.pixel_array) for 'unique id' and 
    """

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

    # crop patient info above US region 

    arr = ds.pixel_array
    
    if is_video:
        arr[:,:y0] = 0
    else:
        arr[:y0] = 0

    ds.PixelData = arr.tobytes()

    return ds

def create_dcm_filename( ds ):
    """uses info from dicom file to create informative filename

    Args:
        ds:  dataset extracted from dicom file

    Returns:
        filename:  "patient id"_"acc num"_"type"_"hash".dcm
             "patient id" is already anonymized by Notion
             "acc num" is already anonymized by Notion
             "type" is image, video (multi-frame array of images), or second (weird type of image, rare)
             "hash" is sha1 hash created from ds.pixel_array (could use to check for duplicates)
    """
    patient_id = ds.PatientID.rjust(8,'0')
    accession_number = ds.AccessionNumber.rjust(8,'0')

    media_type = ds.file_meta[0x00020002]
    is_video = str(media_type).find('Multi-frame')>-1
    is_secondary = str(media_type).find('Secondary')>-1
    
    if is_video:
        media = 'video'
    elif is_secondary:
        media = 'second'
    else:
        media = 'image'

    image_hash = sha1( ds.pixel_array ).hexdigest()

    filename = f'{media}_{patient_id}_{accession_number}_{image_hash}.dcm'

    return filename

def dicom_media_type( dataset ):
    type = str( dataset.file_meta[0x00020002].value )
    if type == '1.2.840.10008.5.1.4.1.1.6.1': # single ultrasound image
        return 'image'
    elif type == '1.2.840.10008.5.1.4.1.1.3.1': # multi-frame ultrasound image
        return 'multi'
    else:
        return 'other' # something else

def extract_deidentify_dcm_files(directory, target_directory):
    
    # Create the target directory if it doesn't exist
    os.makedirs(target_directory, exist_ok=True)
    
    # Get a list of all ZIP files in the directory
    zip_files = [filename for filename in os.listdir(directory) if filename.endswith('.zip') and not filename.startswith('PROC_')]
    
    # Loop over each ZIP file
    for zip_file in zip_files:
        # create target subdirectory
        zip_name, extension = os.path.splitext(zip_file)
        target_subdirectory = target_directory +  zip_name + '_anon/'
        os.makedirs(target_subdirectory, exist_ok = True)
        
        # Open the ZIP file
        zip_path = os.path.join(directory, zip_file)
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            # Loop over each file in the ZIP file
            for member in zip_ref.namelist():
                if member.endswith('.dcm'):

                    # Read the DICOM file using PyDICOM
                    with zip_ref.open(member, 'r') as dicom_file:
                        dataset = pydicom.dcmread(dicom_file)

                        # check to make sure dicom has image or multi-frame video, else ignore
                        # if its an image makde sure SequenceOfUltrasoundRegions is present
                        media_type = dicom_media_type( dataset )
                        if (media_type == 'image' and (0x0018, 0x6011) in dataset) or media_type=='multi':
    
                            # remove patient information from the dicom dataset
                            dataset = deidentify_dicom_dataset(dataset)
    
                            # create new filename from header and hashed image
                            new_filename = create_dcm_filename( dataset ) 
                            
                            # Set the target path to write the DICOM file
                            target_path = os.path.join(target_subdirectory, new_filename)
                        
                            # Write the DICOM dataset to a new DICOM file
                            dataset.save_as(target_path)
        
        # Rename the processed file with 'PROC_' at the beginning
        processed_file = os.path.join(directory, 'PROC_' + zip_file)
        os.rename(zip_path, processed_file)

def check_uncompressed( ds ):
    type = ds.file_meta.TransferSyntaxUID
    uncompressed_types = ['1.2.840.10008.1.2.1','1.2.840.10008.1.2.2','1.2.840.10008.1.2']
    return type in uncompressed_types

def extract_deidentify_dcm_file(zip_path, zip_file, target_directory):
    
    os.makedirs(target_directory, exist_ok = True)
    
    # Open the ZIP file
    full_path_zip_file = os.path.join(zip_path, zip_file)
    with zipfile.ZipFile(full_path_zip_file, 'r') as zip_ref:
        # Loop over each file in the ZIP file
        for member in zip_ref.namelist():
            if member.endswith('.dcm'):

                # Read the DICOM file using PyDICOM
                with zip_ref.open(member, 'r') as dicom_file:
                    #print(dicom_file)
                    dataset = pydicom.dcmread(dicom_file)

                    # check to make sure dicom has image or multi-frame video, else ignore
                    # if its an image makde sure SequenceOfUltrasoundRegions is present
                    media_type = dicom_media_type( dataset )

#                    is_image = media_type == 'image'
                    is_secondary = str(media_type).find('Secondary')>-1
                    if is_secondary:
                        print('SECONDARY:',dicom_file)
                    
                    if (( media_type == 'image' and (0x0018, 0x6011) in dataset) or media_type=='multi'):

                        # if image is compressed, decompress it and change colorspace if needed
                        is_compressed = not check_uncompressed(dataset)
                        if is_compressed:
                            dataset.decompress()
                            arr = dataset.pixel_array
                            color_space_in = dataset.PhotometricInterpretation
                            if color_space_in not in ['MONOCHROME2','RGB']:
                                color_space_out = 'RGB'
                                arr = pydicom.pixel_data_handlers.util.convert_color_space(arr, 
                                                                                           color_space_in, 
                                                                                           color_space_out, 
                                                                                           True)
                                dataset.PixelData = arr.tobytes()
                                dataset.PhotometricInterpretation = color_space_out
                                

                        # remove patient information from the dicom dataset
                        dataset = deidentify_dicom_dataset(dataset)
                        
                        # create new filename from header and hashed image
                        new_filename = create_dcm_filename( dataset ) 
                        
                        # Set the target path to write the DICOM file
                        target_path = os.path.join(target_directory, new_filename)
                    
                        # Write the DICOM dataset to a new DICOM file
                        #print(dicom_file)
                        dataset.save_as(target_path)
    
    # Rename the processed file with 'PROC_' at the beginning
    new_zip_file = os.path.join(zip_path,f'PROC_{zip_file}')
    os.rename(full_path_zip_file, new_zip_file)

def append_to_csv(target_file, input_file):
    # Add prefix "PROC_" to the input filename
    input_dir = os.path.dirname(input_file)
    input_filename = os.path.basename(input_file)
    input_filename_with_prefix = "PROC_" + input_filename
    input_file_with_prefix = os.path.join(input_dir, input_filename_with_prefix)

    # Check if the target file exists
    target_exists = os.path.exists(target_file)

    # Open the input file for reading
    with open(input_file, 'r', newline='') as input_csv_file:
        input_csv_reader = csv.reader(input_csv_file)
        input_rows = list(input_csv_reader)

    # Check if the input file has any rows
    if len(input_rows) == 0:
        print("Input file is empty. No rows to append.")
        return

    # Determine if the target file already has a header
    target_has_header = False
    if target_exists:
        with open(target_file, 'r', newline='') as target_csv_file:
            target_csv_reader = csv.reader(target_csv_file)
            target_has_header = next(target_csv_reader, None) is not None

    # Open the target file for appending
    with open(target_file, 'a', newline='') as target_csv_file:
        target_csv_writer = csv.writer(target_csv_file)

        # If target file doesn't have a header, write the header from the input file
        if not target_has_header:
            target_csv_writer.writerow(input_rows[0])

        # Write the rows from the input file, skipping duplicate rows and the header if it exists
        if target_has_header:
            input_rows = input_rows[1:]

        # Get the existing rows in the target file
        existing_rows = []
        if target_exists:
            with open(target_file, 'r', newline='') as target_csv_file:
                existing_csv_reader = csv.reader(target_csv_file)
                existing_rows = list(existing_csv_reader)

        # Append only the non-duplicate rows
        for row in input_rows:
            if row not in existing_rows and row != input_rows[0]:
                target_csv_writer.writerow(row)
                existing_rows.append(row)

    # Rename the input file with the "PROC_" prefix
    os.rename(input_file, input_file_with_prefix)



def remove_spaces_from_column_names(dataframe):
    dataframe.columns = dataframe.columns.str.replace(' ', '')
    return dataframe

def merge_anon_into_datamart(datamart_csv_file, anon_map_csv_file, target_directory):

    """use anonymization map to replace accession and patient numbers in datamart file, remove patient info, and rename datamart features

    Args:
        datamart_csv_file: full path to unanonymized datamart csv file
        anon_map_csv_file: full path to anonymization map file
        target_directory:  path where anonymized datamart file should be saved (filename derived from input datamart file or save path)

    Returns:
        nothing - output is csv file on disk

    """
 
    datamart_df = remove_spaces_from_column_names( pd.read_csv(datamart_csv_file) )

    print(anon_map_csv_file)
    anon_map_df = remove_spaces_from_column_names( pd.read_csv(anon_map_csv_file) )
    
    datamart_joined_df = pd.merge(datamart_df, anon_map_df,left_on='ACCESSIONNUMBER',right_on='OriginalAccessionNumber',how='left')

    col_to_keep = ['AnonymizedPatientID',
                   'AnonymizedAccessionNumber',
                   'SCORE_CD',
                   'BIOP_SCORE',
                   'SEQ',
                   'A1_PATHOLOGY_TXT',
                   'DENSITY_TXT',
                   'AGE',
                   'RACE',
                   'ETHNICITY']
    
    new_names = {"AGE":"Age",
                 "RACE":"Race",
                 "ETHNICITY":"Ethnicity",
                 "DENSITY_TXT":"Density_Desc",
                 "A1_PATHOLOGY_TXT":"Path_Desc",
                 "SCORE_CD":"BI-RADS",
                 "SEQ":"Biop_Seq",
                 "BIOP_SCORE":"Biopsy",
                 "AnonymizedPatientID":"Patient_ID",
                 "AnonymizedAccessionNumber":"Accession_Number"}
                   
    datamart_joined_df = datamart_joined_df[col_to_keep]
    datamart_joined_df.rename(columns = new_names, inplace=True)
    datamart_joined_df.sort_values(by=['Patient_ID','Accession_Number'], inplace=True)

    filename = 'datamart_anon.csv'
    full_path = os.path.join(target_directory, filename)

    datamart_joined_df.to_csv(full_path, index = False)
    

def concat_csv_files(directory_path, target_file, remove_target=False):
    """
    Concatenate CSV files from a directory (without PROC_ prefix) into a target CSV file.

    Args:
        directory_path (str): Path to the directory containing CSV files.
        target_file (str): Path to the target CSV file where merged data will be saved.
        remove_target (bool, optional): If True, the target file will be removed before appending. Default is False.
    """

    # Remove the target file if it exists and remove_target is True
    if remove_target and os.path.exists(target_file):
        os.remove(target_file)

    # Get a list of CSV files without the PROC_ prefix
    csv_files = [file for file in os.listdir(directory_path) if file.endswith('.csv') and not file.startswith('PROC_')]

    # Initialize a set to store the appended rows to avoid duplicates
    appended_rows = set()

    # If the target file exists, read its contents and populate the set
    if os.path.exists(target_file):
        with open(target_file, 'r', newline='') as target:
            target_reader = csv.reader(target)
            appended_rows.update(tuple(row) for row in target_reader)

    # Loop over each CSV file and append its unique contents to the target file
    with open(target_file, 'a', newline='') as target:
        target_writer = csv.writer(target)

        for index, file in enumerate(csv_files):
            file_path = os.path.join(directory_path, file)
            with open(file_path, 'r', newline='') as source:
                source_reader = csv.reader(source)

                # Skip the first row (headers) of all files except the first file
                if index > 0:
                    next(source_reader)

                # Append the unique contents of the current CSV file to the target file
                for row in source_reader:
                    row_tuple = tuple(row)
                    if row_tuple not in appended_rows:
                        target_writer.writerow(row)
                        appended_rows.add(row_tuple)

            # Add a PROC_ prefix to the current CSV file after it's appended
            os.rename(file_path, os.path.join(directory_path, 'PROC_' + file))
            
            
            
def get_dcm_files(directory):
    dcm_files = []
    
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith('.dcm'):
                dcm_files.append(os.path.join(root, file))
    
    return dcm_files
            
            
# this was one-time code for converting all the datamart split files
# to notion query files, keep the code in case notion rejects the converted files

def datamart_to_notion_query( datamart_file, notion_query_file):
    """read datamart csv and write out notion_query xlsx file

    Args:
        datamart_file:  string with full path filename to datamart file (csv)
        notion_query_file:  target filename (xlsx)

    Returns:
        none

    """
    datamart_df = pd.read_csv( datamart_file )

    col_names = [
        'PatientName', 'PatientID', 'AccessionNumber', 
        'PatientBirthDate', 'StudyDate', 'ModalitiesInStudy', 
        'StudyDescription', 'AnonymizedName', 'AnonymizedID']
    
    notion_query_df = pd.DataFrame(
        {'PatientID': datamart_df['PATIENTID'],
	     'AccessionNumber': datamart_df['ACCESSIONNUMBER']}, 
        columns=col_names)

    notion_query_df.to_excel(notion_query_file, index=False)
    

def append_to_csv(target_file, input_file, columns):
    # Add prefix "PROC_" to the input filename
    input_dir = os.path.dirname(input_file)
    input_filename = os.path.basename(input_file)
    input_filename_with_prefix = "PROC_" + input_filename
    input_file_with_prefix = os.path.join(input_dir, input_filename_with_prefix)

    # Check if the target file exists
    target_exists = os.path.exists(target_file)

    # Open the input file for reading
    with open(input_file, 'r', newline='') as input_csv_file:
        input_csv_reader = csv.reader(input_csv_file)
        input_rows = list(input_csv_reader)

    # Check if the input file has any rows
    if len(input_rows) == 0:
        print("Input file is empty. No rows to append.")
        return

    # Determine if the target file already has a header
    target_has_header = False
    if target_exists:
        with open(target_file, 'r', newline='') as target_csv_file:
            target_csv_reader = csv.reader(target_csv_file)
            target_has_header = next(target_csv_reader, None) is not None

    # Open the target file for appending
    with open(target_file, 'a', newline='') as target_csv_file:
        target_csv_writer = csv.writer(target_csv_file)

        # If target file doesn't have a header, write the header from the input file
        if not target_has_header:
            target_csv_writer.writerow(input_rows[0])

        # Write the rows from the input file, skipping duplicate rows and the header if it exists
        if target_has_header:
            input_rows = input_rows[1:]

        # Get the existing rows in the target file
        existing_rows = []
        if target_exists:
            with open(target_file, 'r', newline='') as target_csv_file:
                existing_csv_reader = csv.reader(target_csv_file)
                existing_rows = list(existing_csv_reader)

        # Append only the non-duplicate rows
        for row in input_rows:
            match_found = False
            for existing_row in existing_rows:
                if all(existing_row[col] == row[col] for col in columns):
                    match_found = True
                    break

            if not match_found:
                target_csv_writer.writerow(row)
                existing_rows.append(row)

    print("Rows appended successfully!")

    # Rename the input file with the "PROC_" prefix
    os.rename(input_file, input_file_with_prefix)
    print("Input file renamed with prefix:", input_file_with_prefix)
    





def extract_single_zip_file(file_name, output_folder):
    if os.path.exists(output_folder):
        return

    try:
        zip_ref = zipfile.ZipFile(file_name)  # create zipfile object
        zip_ref.extractall(output_folder)  # extract file to dir
        zip_ref.close()  # close file
    except Exception as e:
        print(f'Skipping Bad Zip File: {file_name}. Exception: {e}')

def unzip_files_in_directory(directory_path):
    target_dir = os.path.join(directory_path, 'unzipped_dicoms')
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




#zip_dicom_path = 'D:/DATA/CASBUSI/new_batch_(delete_me)'
#unzip_files_in_directory(zip_dicom_path)




current_dir = "F:/CODE/CASBUSI/CASBUSI-Mayo/mayo-dev/"
            
path_anon_maps = f'{current_dir}/data_orig/notion_anon_maps/'
path_datamart_splits = f'{current_dir}/data_orig/datamart_splits/processed/'
path_notion_queries = f'{current_dir}/data_orig/notion_queries/'
path_datamart_master = f'{current_dir}/data_orig/'
path_anon_map_master = f'{current_dir}/data_orig/'
path_data_anon = f'{current_dir}/data_anon/'

datamart_master_file = 'master_datamart.csv'
anon_map_master_file = 'master_anon_map.csv'

full_path_datamart_master_file = os.path.join(path_datamart_master, datamart_master_file)
full_path_anon_map_master_file = os.path.join(path_anon_map_master, anon_map_master_file)


directory_path = path_anon_maps
target_file = full_path_anon_map_master_file
#concat_csv_files(directory_path, target_file, remove_target=True)



# get list of unprocessed datamart_split files
datamart_splits_files = [filename for filename in os.listdir(path_datamart_splits) if filename.endswith('.csv') and not filename.startswith('PROC')]

# main processing loop
for datamart_file in datamart_splits_files:
    # get batch number
    batch_number_str = datamart_file.split('_')[0]

    # process the zip files, deidentify the dicoms, and add them to target directory    
    dicoms_zip_file = f'{batch_number_str}_dicoms.zip'
    target_directory = os.path.join(path_data_anon, f'{batch_number_str}_dicoms_anon/')
    extract_deidentify_dcm_file(zip_dicom_path, dicoms_zip_file, target_directory)

    # merge anon_map into datamart split file and clean out all PHI
    datamart_csv_file = os.path.join(path_datamart_splits,f'{batch_number_str}_datamart.csv')
    merge_anon_into_datamart(datamart_csv_file, full_path_anon_map_master_file, target_directory)
    
    # add datamart split file to master datamart file and add PROC_ prefix
    append_to_csv( full_path_datamart_master_file, datamart_csv_file)
    
# merge anon_map into datamart split file and clean out all PHI
datamart_csv_file = os.path.join(path_datamart_splits,f'{batch_number_str}_datamart.csv')
merge_anon_into_datamart(datamart_csv_file, full_path_anon_map_master_file, target_directory)

append_to_csv( full_path_datamart_master_file, datamart_csv_file)



dcm_files = get_dcm_files('./data_anon')

print(f'There were {len(dcm_files)} dicom files found.')


for batch in np.arange(19,127):
    batch_string = f'{batch:05}'
    datamart_file = f'./data_orig/datamart_splits/unprocessed/{batch_string}_datamart.csv'
    notion_query_file = f'./data_orig/notion_queries/{batch_string}_notion_query.xlsx'
    datamart_to_notion_query( datamart_file, notion_query_file )
    
    

# Example usage
target_csv_file = 'target.csv'
input_csv_file = 'input.csv'
columns_to_check = [0, 1]  # Example columns to check (0-based indices)

append_to_csv(target_csv_file, input_csv_file, columns_to_check)


ds_problem1 = pydicom.dcmread('./problem.dcm')
ds_problem2 = pydicom.dcmread('./problem2.dcm')
ds_no_problem = pydicom.dcmread('./no_problem.dcm')
ds_problem3 = pydicom.dcmread('./problem3.dcm')

ds_new = deidentify_dicom_dataset(ds_problem3)

print(ds_new)