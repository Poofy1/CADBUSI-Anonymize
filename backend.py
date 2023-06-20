import os
from typing import Any

import numpy as np
import pydicom
from numpy import uint8
from numpy.typing import NDArray
import deidentify
import json
import pathlib
from typing import List, Union

from pony.orm import Database, Json, PrimaryKey, Required, Optional, Set, db_session
from pydantic import BaseModel, validator

import asyncio
import hashlib
import io
import tempfile
import shutil

from datetime import datetime
import numpy as np
import pandas as pd
import pydicom
from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.responses import FileResponse, Response
from PIL import Image, ImageFile
from zipfile import ZipFile
import redis

#Dictionary that holds the notion data
cases = {}

# Pixel value threshold for creating masks
PIXEL_THRESHOLD = 150

# Maximum height of an image can be before we extract text, we crop it if
# it is over.
MAXIMUM_VERTICAL_CROP = 500


def dicom_to_dict(dicom: pydicom.FileDataset |
                  pydicom.Dataset) -> dict[str, Any]:
    """
    Convert DICOM metadata to python dictionary.

    :param dicom: DICOM dataset to parse
    :type dicom: pydicom.FileDataset | pydicom.Dataset
    :return: dictionary of metadata
    :rtype: dict[str, Any]
    """
    data = {}
    for elem in dicom.elements():
        if elem.keyword == "PixelData":
            continue
        if isinstance(elem.value, pydicom.Sequence):
            data[str(elem.keyword)] = [dicom_to_dict(sub_dicom)
                                       for sub_dicom in elem.value]
        match elem.value:
            case pydicom.Sequence():
                val = \
                    [dicom_to_dict(sub_dicom) for sub_dicom in elem.value]
            case pydicom.multival.MultiValue():  # type: ignore
                val = list(elem.value)
            case pydicom.valuerep.DSfloat():  # type: ignore
                val = float(elem.value)
            case pydicom.valuerep.IS():  # type: ignore
                val = int(elem.value)
            case pydicom.valuerep.PersonName():  # type: ignore
                val = str(elem.value)
            case pydicom.uid.UID():  # type: ignore
                val = str(elem.value)
            case _:
                val = elem.value
        data[str(elem.keyword)] = val
    return data




def deidentify_image(self, src_path, dest_path):  # pylint: disable=unused-argument
    dest_dir_path, dest_filename = os.path.split(dest_path)
    print(f'SRC_PATH: {src_path} DEST_PATH: {dest_path}')
    print(f'SRC_PATH: {src_path} DEST_DIR_PATH: {dest_dir_path} DEST_FILENAME: {dest_filename}')

    text_extracted = deidentipy.run(src_path, dest_dir_path + '/', dest_filename)
    
    return True





DATA_PATH = 'data'
DATA_EXPORT_PATH = '/usr/data/export'
DATABASE_FILENAME = 'db.sqlite3'

INGEST_ZIPFILE_PATH = '/usr/data/zip_archives'
DATA_ZIPFILE_PATH = '/usr/src/app/data/zip'


# Bind database to file and load/create tables
if not os.path.exists(DATABASE_FILENAME):
    with open(DATABASE_FILENAME, 'xb') as database_file:
        pass

# Create data storage directory structure if not already exists
folder_paths = [
    'data',
    'data/csv', 
    'data/csv/datamart',
    'data/csv/notion',
    'data/dicoms',
    'data/images',
    'data/images/source',
    'data/images/cropped',
    'data/zip',
]
for folder_path in folder_paths:
    if not os.path.exists(folder_path):
        os.mkdir(folder_path)


db = Database()
db.bind(provider='sqlite', filename=DATABASE_FILENAME)


class Case(db.Entity): 
    accession_num = PrimaryKey(int)
    mrn = Required(int)
    biopsy = Required(str)
    birads = Required(str)
    datamart_filename = Required(str)
    datamart_filepath = Required(str)
    datamart_vars = Required(str)

    anonymized_accession_num = Optional(int)
    notion_filename = Optional(str)
    notion_filepath = Optional(str)

    dicoms = Set("Dicom")

class Dicom(db.Entity):  # type: ignore
    # noqa
    id = PrimaryKey(int, auto=True)
    case = Required(Case)

    # Dicom info
    filename = Required(str)
    dicom_hash = Required(str, unique=True)
    image_hash = Required(str, unique=True)
    metadata = Required(Json)

    @property
    def local_dicom_filename(self) -> str:
        # noqa
        return f'{self.id:06}_{self.filename}'

    @property
    def local_dicom_filepath(self) -> pathlib.Path:
        # noqa
        path = f'{DATA_PATH}/dicoms/{self.local_dicom_filename}'
        return pathlib.Path(path)

    @property
    def local_source_filename(self) -> str:
        # noqa
        return f'{self.id:06}_source.png'

    @property
    def local_source_filepath(self) -> pathlib.Path:
        # noqa
        path = f'{DATA_PATH}/images/source/{self.local_source_filename}'
        return pathlib.Path(path)

    @property
    def local_cropped_filename(self) -> str:
        # noqa
        return f'{self.id:06}_cropped.png'

    @property
    def local_cropped_filepath(self) -> pathlib.Path:
        # noqa
        path = f'{DATA_PATH}/images/cropped/{self.local_cropped_filename}'
        return pathlib.Path(path)
    
    
class DicomSchema(BaseModel):
    # noqa
    id: str

    # Dicom info
    filename: str
    dicom_hash: str
    image_hash: str
    metadata: dict

    @validator('metadata', pre=True)
    def load_metadata(cls, value):
        if value == {}:
            return ""
        print("metadata type:", type(value))
        # print("metadata:", value)
        return json.loads(value)

    class Config:
        orm_mode = True


class CaseSchema(BaseModel):
    # Patient info
    mrn: int
    accession_num: int
    anonymized_accession_num: int
    biopsy: str
    birads: str

    # Datamart and Notion info
    datamart_filename: str
    datamart_filepath: str
    notion_filename: str
    notion_filepath: str
    datamart_vars: dict

    dicoms: List[dict]

    @validator('datamart_vars', pre=True)
    def load_datamart_vars(cls, value):
        if value == {}:
            return ""
        return json.loads(value)

    @validator('anonymized_accession_num', pre=True)
    def unwrap_anonymized_accession_num(cls, value):
        return -1 if value is None else value

    @validator('notion_filename', pre=True)
    def unwrap_notion_filename(cls, value):
        return "" if value is None else value

    @validator('notion_filepath', pre=True)
    def unwrap_notion_filepath(cls, value):
        return "" if value is None else value

    @validator('dicoms', pre=True, allow_reuse=True)
    def dicoms_set_to_list(cls, values):
        return [dict(DicomSchema.from_orm(dicom)) for dicom in values]

    class Config:
        orm_mode = True








# Initialize fastAPI
app = FastAPI()

# Connect to redis database
redis_conn = redis.Redis(host='redis', port=6379, db=1)

# Create/Load database
db.generate_mapping(create_tables=True)

# Allow use of truncated images
ImageFile.LOAD_TRUNCATED_IMAGES = True


def deidentify_case_dict(case_dict):
    case_dict.pop('mrn')
    case_dict.pop('accession_num')

    # TODO: deidentify new columns in datamart file, I removed everything for safety for now
    case_dict['datamart_vars']['ACCESSION'] = ''
    case_dict['datamart_vars'] = {}

    return case_dict

def deidentify_dicom_dict(dicom_dict):
    dicom_dict['metadata']['PatientBirthDate'] = ''
    dicom_dict['metadata']['PatientAge'] = ''
    return dicom_dict

def unzip_file(filepath: str, notion_filepath: str):
    env = os.path.dirname(os.path.abspath(__file__))
    output_path = f"{env}/zip_output"
    
    if not os.path.exists(output_path):
        os.makedirs(output_path, exist_ok=True)
        
    filename = os.path.basename(filepath)
    print("Unzipping file:", filename)

    notion_filename = os.path.basename(notion_filepath)

    with ZipFile(filepath, 'r') as zipfp:
        num_files = len(list(zipfp.namelist()))
        for i, subfile in enumerate(zipfp.filelist):
            if not subfile.filename.endswith('dcm'):
                continue

            with zipfp.open(subfile.filename, 'r') as subfp:
                data = io.BytesIO(subfp.read())
                upload_dicom(notion_filename, f'{os.path.splitext(filename)[0]}_{i:05}.dcm', data)

    shutil.copyfile(filepath, f'{output_path}/{filename}')



def fetch_dicom_files():
    with db_session:
        dicoms = Dicom.select()
        result = [dict(DicomSchema.from_orm(dicom)) for dicom in dicoms]
        for dicom_dict, dicom_obj in zip(result, dicoms):
            dicom_dict['accession_num'] = dicom_obj.case.accession_num
            dicom_dict['mrn'] = dicom_obj.case.mrn
            dicom_dict['biopsy'] = dicom_obj.case.biopsy
            dicom_dict['datamart_filename'] = dicom_obj.case.datamart_filename
            dicom_dict['datamart_filepath'] = dicom_obj.case.datamart_filepath
            dicom_dict['anonymized_accession_num'] = dicom_obj.case.anonymized_accession_num
            dicom_dict['notion_filename'] = dicom_obj.case.notion_filename
            dicom_dict['notion_filepath'] = dicom_obj.case.notion_filepath
    return result


def upload_datamart(file_path: str, DATA_PATH: str) -> Union[str, None]:
    # Check that file exists
    if not os.path.exists(DATA_PATH):
        os.makedirs(DATA_PATH, exist_ok=True)
    
    if not os.path.exists(file_path):
        print('Missing datamart file')
        return None

    # Read content of file
    with open(file_path, "rb") as f:
        content = f.read()

    # Get hash for datamart file
    datamart_hash_value = hashlib.md5(content).hexdigest()

    # Create filenames
    timestamp = datetime.now().strftime("%Y_%m_%d-%I_%M_%S_%p")
    notion_filename = f"notion_query_{timestamp}.csv"
    datamart_dir = f"{DATA_PATH}/{datamart_hash_value}"
    print("DIR:", datamart_dir, " ISDIR:", os.path.isdir(datamart_dir))
    if os.path.isdir(datamart_dir):
        # Get existing filename and load it
        datamart_filename = os.listdir(datamart_dir)[0]
        datamart_df = pd.read_csv(f'{datamart_dir}/{datamart_filename}')
    else:
        # Create new filename
        datamart_filename = f"datamart_{timestamp}.csv"

        # Make new directory for hash value
        os.mkdir(datamart_dir)

        # Read file into dataframe and create new dataframe for Notion query
        datamart_df = pd.read_csv(io.BytesIO(content))
        datamart_df.to_csv(f'{datamart_dir}/{datamart_filename}', index=False)

    # Save datamart information to database
    with db_session:
        for i, row in datamart_df.iterrows():
            if Case.select(lambda c: c.accession_num == row['ACCESSIONNUMBER']).exists():
                continue
            case_obj = Case(
                mrn = row['PATIENTID'],
                accession_num = row['ACCESSIONNUMBER'],
                biopsy = str(row['BIOP_SCORE']),
                birads = str(row['SCORE_CD']),
                datamart_filename = datamart_filename,
                datamart_filepath = f'{datamart_dir}/{datamart_filename}',
                datamart_vars = row.to_json()
            )

    # Create new dataframe for Notion query
    col_names = [
        'PatientName', 'PatientID', 'AccessionNumber', 
        'PatientBirthDate', 'StudyDate', 'ModalitiesInStudy', 
        'StudyDescription', 'AnonymizedName', 'AnonymizedAccessionNumber', 
        'OriginalAccessionNumber', 'mrn', 'biopsy', 'birads', 'datamart_filename',
        'datamart_filepath', 'datamart_vars']
    notion_query_df = pd.DataFrame(
        {'PatientID': datamart_df['PATIENTID'],
	    'AccessionNumber': datamart_df['ACCESSIONNUMBER'],
        'biopsy' : str(row['BIOP_SCORE']),
        'birads' : str(row['SCORE_CD']),
        'datamart_filename' : datamart_filename,
        'datamart_filepath' : f'{datamart_dir}/{datamart_filename}',
        'datamart_vars' : row.to_json()}, 
        columns=col_names)

    print("NOTION QUERY DF:", notion_query_df)

    # Save dataframe to specified folder
    output_file_path = f"{DATA_PATH}/notion_query_{timestamp}.csv"
    notion_query_df.to_csv(output_file_path, index=False)

    return output_file_path





def upload_notion(zip_filepath, notion_file_path, INPUT_PATH, ZIP_INPUT, OUTPUT_PATH):
    # Check that file exists
    if not os.path.exists(OUTPUT_PATH):
        os.makedirs(OUTPUT_PATH, exist_ok=True)
    
    # Read content of file
    with open(f"{INPUT_PATH}/{notion_file_path}", "rb") as f:
        content = f.read()

    assert isinstance(content, bytes), "File contents were not bytes."

    # Get hash for datamart file
    notion_hash_value = hashlib.md5(content).hexdigest()

    # Create filenames
    timestamp = datetime.now().strftime("%Y_%m_%d-%I_%M_%S_%p")
    notion_dir = f"{OUTPUT_PATH}/{notion_hash_value}"
    if os.path.isdir(notion_dir):
        # Get existing filename and load it
        notion_filename = os.listdir(notion_dir)[0]
        notion_df = pd.read_csv(f'{notion_dir}/{notion_filename}')
    else:
        # Create new filename
        notion_filename = f"notion_{timestamp}.csv"

        # Make new directory for hash value
        os.mkdir(notion_dir)

        # Read file into dataframe and create new dataframe for Notion query
        notion_df = pd.read_csv(io.BytesIO(content))
        notion_df.to_csv(f'{notion_dir}/{notion_filename}', index=False)

    # DEBUG TEMP CODE
    notion_df['OriginalAccessionNumber'] = notion_df['OriginalAccessionNumber'].fillna(0)
    notion_df['AnonymizedAccessionNumber'] = notion_df['AnonymizedAccessionNumber'].fillna(0)
    notion_df['OriginalAccessionNumber'] = notion_df['OriginalAccessionNumber'].astype(int)
    notion_df['AnonymizedAccessionNumber'] = notion_df['AnonymizedAccessionNumber'].astype(int)
    
    # Use a dictionary to save notion information
    for i, row in notion_df.iterrows():
        case = {
            'accession_num': int(row['OriginalAccessionNumber']),
            'anonymized_accession_num': int(row['AnonymizedAccessionNumber']),
            'notion_filename': notion_filename,
            'notion_filepath': f'{OUTPUT_PATH}/{notion_filename}'
        }

        case_key = int(row['OriginalAccessionNumber'])
        cases[case_key] = case

        print(f"CASE OBJ CREATED - ID: {int(row['AnonymizedAccessionNumber'])}")

    # Unzip file
    unzip_file(f"{ZIP_INPUT}/test_zip.zip", f'{notion_dir}/{notion_filename}')











async def upload_video(dicom, dicom_hash):
    file_id = dicom_hash
    ds = dicom
    video_arr = np.array(ds.pixel_array)
    number_of_frames = ds['NumberOfFrames'].value

    output_path = f'{DATA_EXPORT_PATH}/video/{file_id}'

    if os.path.exists(output_path):
        shutil.rmtree(output_path)
    os.makedirs(output_path)

    # Write metadata to file
    with open(f'/usr/data/export/video/{file_id}/metadata.json', "w") as fout:
        dicom_dict = tools.dicom_to_dict(dicom)

        dicom_dict = deidentify_dicom_dict(dicom_dict)

        json.dump(dicom_dict, fout)

    for i in range(number_of_frames):
        im = Image.fromarray(video_arr[i])
        top_of_ultrasound = dicom['SequenceOfUltrasoundRegions'][0]['RegionLocationMinY0'].value
        im_arr = np.array(im)
        im_arr[:top_of_ultrasound, :] = 0
        im_cropped = Image.fromarray(im_arr)
        im_cropped.save(f'/usr/data/export/video/{file_id}/{i}.png')


def upload_dicom(notion_filename: str, output_filename: str, file: UploadFile = File(...)) -> dict[str, str, str]:


    # Read content of file
    content = file.read()
    # Rewind the file pointer to the start of the file
    file.seek(0)

    # Read and decode DICOM file
    dicom = pydicom.dcmread(file)

    with db_session:
        # Check if hash already exists in database
        dicom_hash_value = hashlib.md5(content).hexdigest()
        matching_dicom_hashes = Dicom.select(
            lambda d: d.dicom_hash == dicom_hash_value)
        if len(matching_dicom_hashes) > 0:
            print(f'DICOM file already exists: {output_filename}')

        # Read and decode DICOM file
        dicom = pydicom.dcmread(io.BytesIO(content))
        dicom.decode()
        assert isinstance(dicom, pydicom.FileDataset), \
            'Expected FileDataset, recieved DicomDir'

        # Check if video file
        if ('0028', '0008') in list(dicom._dict.keys()):
            upload_video(dicom, dicom_hash_value)
            return {
                'filename': output_filename
            }

        # Check if image in dicom file already exists in database
        image_hash_value = hashlib.md5(dicom.pixel_array.tobytes()).hexdigest()
        matching_image_hashes = Dicom.select(
            lambda d: d.image_hash == image_hash_value)
        if len(matching_image_hashes) > 0:
            print(f'DICOM with matching image already exists: {output_filename}')
        print("Image hash value unique")

        # Open image file
        im = Image.fromarray(dicom.pixel_array)

        # Top-cropped pseudo-deidentified image to disk
        top_of_ultrasound = dicom['SequenceOfUltrasoundRegions'][0]['RegionLocationMinY0'].value
        im_arr = np.array(im)
        if len(im_arr.shape) == 2:
            im_arr[:top_of_ultrasound, :] = np.zeros(1, dtype=np.uint8)
        else:
            im_arr[:top_of_ultrasound, :] = np.zeros(
                im_arr.shape[2], dtype=np.uint8)
        im_cropped = Image.fromarray(im_arr)
        print("Image successfully cropped")

        # Save DICOM file information to database
        print("Save Dicom Information")
        metadata = json.dumps(dicom_to_dict(dicom))
        print("Metadata successfully extracted")
        

        # Find the matching case from the cases dictionary
        case_obj = None
        print(cases.values())
        for case in cases.values():
            if case['notion_filename'] == notion_filename: #and case['anonymized_accession_num'] == dicom.AccessionNumber:
                case_obj = Case(
                    accession_num=case.get('accession_num', None),
                    mrn=case.get('mrn', 1),
                    biopsy=case.get('biopsy', '1'),
                    birads=case.get('birads', '1'),
                    datamart_filename=case.get('datamart_filename', '1'),
                    datamart_filepath=case.get('datamart_filepath', '1'),
                    datamart_vars= json.dumps(case.get('datamart_vars', {})),
                    anonymized_accession_num=case.get('anonymized_accession_num', 1),
                    notion_filename=case.get('notion_filename', '1'),
                    notion_filepath=case.get('notion_filepath', '1')
                )
                break

        if not case_obj:
            print(f"No Case found for notion_filename={notion_filename} and anonymized_accession_num={dicom.AccessionNumber}")
            return {'filename': output_filename, 'status': 'No matching Case record found'}

        # Create CustomDicom instance
        dicom_obj = Dicom(
            case=case_obj,
            filename=output_filename,
            dicom_hash=dicom_hash_value,
            image_hash=image_hash_value,
            metadata=metadata
        )
        print("Custom Dicom Obj Created:", dicom_obj)

    # Save dicom file to disk
    with open(dicom_obj.local_dicom_filepath, 'wb') as dicom_file:
        dicom_file.write(content)

    # Save image file to disk
    im.save(dicom_obj.local_source_filepath)

    # Save cropped image file to disk
    im_cropped.save(dicom_obj.local_cropped_filepath)

    return {'filename': output_filename}


def export_data():
    env = os.path.dirname(os.path.abspath(__file__))
    final_output = f'{env}/final_output'
    if not os.path.exists(final_output):
        os.makedirs(final_output, exist_ok=True)
    
    with db_session:
        cases = Case.select()
        result = [dict(CaseSchema.from_orm(case)) for case in cases]
        # dicoms = Dicom.select()
        # result = [dict(DicomSchema.from_orm(dicom)) for dicom in dicoms]
        
        # Save full database to data folder for backup
        with open(f'{env}/zip_output/database.json', 'w') as json_fp:
            json.dump(result, json_fp)
        
        # Remove PHI from database json
        for i, case_dict in enumerate(result):

            case_dict = deidentify_case_dict(case_dict)
            result[i] = case_dict
            
            for i, dicom_dict in enumerate(case_dict['dicoms']):
                case_dict['dicoms'][i] = deidentify_dicom_dict(dicom_dict)

        # Save deidentified databse to export folder
        with open(f'{final_output}/database.json', 'w') as json_fp:
            json.dump(result, json_fp)

        df_data = []
        if not os.path.exists(f'{final_output}/image'):
            os.makedirs(f'{final_output}/image')

        
        for case_dict, case_obj in zip(result, cases):
            for dicom_dict, dicom_obj in zip(case_dict['dicoms'], case_obj.dicoms):

                df_data.append({
                    'id': dicom_dict['id'],
                    'filename': dicom_dict['filename'],
                    'dicom_hash': dicom_dict['dicom_hash'],
                    'image_hash': dicom_dict['image_hash'],
                    'anonymized_accession_num': case_dict['anonymized_accession_num'],
                    'biopsy': case_dict['biopsy'],
                    'birads': case_dict['birads'],
                })

                if os.path.exists(dicom_obj.local_cropped_filepath):
                    shutil.copyfile(
                        dicom_obj.local_cropped_filepath,
                        f'{final_output}/image/{dicom_obj.local_cropped_filename}')


        db_df = pd.DataFrame(df_data)
        db_df.to_csv(f'{final_output}/database.csv', index=False)

    return result