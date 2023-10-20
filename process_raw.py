import pandas as pd
from tqdm import tqdm
import os
env = os.path.dirname(os.path.abspath(__file__))




def read_supported_file(filename):
    # Extract the file extension
    file_extension = filename.split(".")[-1].lower()

    # Check if the file extension is supported
    if file_extension in ['xlsx', 'csv']:
        if file_extension == 'xlsx':
            try:
                df = pd.read_excel(filename)
                return df
            except Exception as e:
                return f"Error reading XLSX file: {e}"
        elif file_extension == 'csv':
            try:
                df = pd.read_csv(filename)
                return df
            except Exception as e:
                return f"Error reading CSV file: {e}"
    else:
        return "File type not supported. Supported file types: xlsx, csv"


def biop_modality( biop_description ):
    if ('ULTRASOUND') in biop_description or ('US' in biop_description):
        return 'US'
    if ('STEREO' in biop_description) or ('TOMO' in biop_description) :
        return 'X-RAY'
    if 'MR' in biop_description:
        return 'MRI'
    
# add laterality column
def add_laterality_column(dataframe, target_column):
    laterality_mapping = {
        'LEFT': 'LEFT',
        'RIGHT': 'RIGHT',
        'BILATERAL': 'BILATERAL'
    }

    def get_laterality(value):
        for key, laterality in laterality_mapping.items():
            if key in value.upper():
                return laterality
        return 'UNKNOWN'

    dataframe['Laterality'] = dataframe[target_column].apply(get_laterality)
    return dataframe


def match_biops_to_exam( df, exam_index, days_before = -30, days_after = 120):
    '''
    Args:
        df: dataframe containing biopsy and ultrasound study details for one patient
            this dataframe is a slice of the total dataframe and the indices work for that frame
        exam_index: the index of the exam we are trying to match
        days_before:  max days before exam date for related biopsy (negative)
        days_after: max days after exam date for related biopsy
        
    Returns:
        matches_left_ind:  list of indices of matching left biopsies, list contains single negative number for time to next future match
        matches_right_ind: list of indices of matching right biopsies, ...
    '''
    
    exam_date = df.loc[exam_index,'Date']
    exam_lat = df.loc[exam_index,'Laterality']
    
    df_biops = df[ df['BIOPSY'] ]
    biop_dates = df_biops['Date']
    biop_lats = df_biops['Laterality']
    
    days = (biop_dates-exam_date).dt.total_seconds()/86400 # convert to days
    matches_left_df = df_biops[(days >= days_before ) & (days <= days_after) & (biop_lats=='LEFT')]
    matches_right_df = df_biops[(days >= days_before ) & (days <= days_after) & (biop_lats=='RIGHT')]
    matches_left_ind = matches_left_df.index.to_list()
    matches_right_ind = matches_right_df.index.to_list()

    if len(matches_left_ind)==0: # find time to next left future match
        days_left = days[ biop_lats=='LEFT' ]
        days_left = days_left[ days_left >= 0].to_list()
        if len(days_left)==0:

            matches_left_ind = [-9999]
        else:
            matches_left_ind = [-max(days_left)]

    if len(matches_right_ind)==0:
        days_right = days[ biop_lats=='RIGHT']
        days_right = days_right[ days_right >= 0].to_list()
        if len(days_right)==0:
            matches_right_ind = [-9999]
        else:
            matches_right_ind = [-max(days_right)]

    if exam_lat=='LEFT':
        matches_right_ind = []
    if exam_lat=='RIGHT':
        matches_left_ind = []

    return matches_left_ind, matches_right_ind







df = pd.read_csv(f"{env}/input_file/data_complete.csv", low_memory=False)

cols_to_keep = ['Patient Clinic Nbr',
                'Accession Nbr Id',
                'Final Status Dt',
                'PATIENT_GENDER_CODE',
                'PRoc_Name',
                'SCORE_CD',
                'LOCATION_SITE_NAME', 
                'Age at Final Status Date',
                'PATIENT_RACE_NAME', 
                'PATIENT_ETHNICITY_NAME',
                'DENSITY_TXT (Custom SQL Query2)',
                'A1_PATHOLOGY_TXT',
                'A1_PATHOLOGY_CATEGORY_DESC']

df = df[cols_to_keep]

rename_dict = {'Patient Clinic Nbr':'PatientID',
               'Accession Nbr Id':'Accession',
               'Final Status Dt':'Date',
               'PATIENT_GENDER_CODE':'Gender',
               'PRoc_Name':'Procedure',
               'SCORE_CD':'BI-RADS',
               'LOCATION_SITE_NAME':"Location", 
               'Age at Final Status Date':"Age",
               'PATIENT_RACE_NAME':'Race', 
               'PATIENT_ETHNICITY_NAME':'Ethnicity',
               'DENSITY_TXT (Custom SQL Query2)':"Density",
               'A1_PATHOLOGY_TXT':"Path_Note",
               'A1_PATHOLOGY_CATEGORY_DESC':"Pathology"}

df.rename( columns = rename_dict, inplace = True )
df = df[ ~(df['Location']=='UNKNOWN') ]
df['BIOPSY'] = df['Procedure'].apply(lambda x: 'BIOPSY' in x)
df = df.sort_values(['PatientID','Accession'])
df = df.reset_index(drop=True)


# convert the 'Date' column to datetime format
df['Date']= pd.to_datetime(df['Date'])
df['Accession'] = df['Accession'].astype(int)
df = add_laterality_column(df, 'Procedure')


df_slice = df[ df['PatientID'] == 736585]
exam_index = 1
left,right = match_biops_to_exam( df_slice, exam_index )

# find matching biopsies for every exam
all_exam_indices = df[ ~df['BIOPSY'] ].index.to_list()
biopsy_mapping = {}


for exam_index in tqdm(all_exam_indices):
    patient_id = df.loc[exam_index,'PatientID']
    df_slice = df[ df['PatientID'] == patient_id]
    left,right = match_biops_to_exam( df_slice, exam_index )
    biopsy_mapping[exam_index] = {'LEFT':left, 'RIGHT':right}

# now use mapping dictionary to construct dataframe of exams and matching biopsies 
df_match = pd.DataFrame(columns=['PatientID', 
                                 'Accession',
                                 'Age',
                                 'Race',
                                 'Ethnicity',
                                 'Density',
                                 'Gender',
                                 'BI-RADS',
                                 'Exam_Date',
                                 'Exam_Laterality',
                                 'Time_Biop',
                                 'Biop_Laterality', 
                                 'Biop_Accession', 
                                 'Biop_Date', 
                                 'Biop_Procedure',
                                 'Biop_Modality',
                                 'Path_Note',
                                 'Pathology'])


for exam_index in tqdm(all_exam_indices):
    # get exam information from master df
    PatientID = df.loc[exam_index,'PatientID']
    Accession = df.loc[exam_index,'Accession']
    Age = df.loc[exam_index,'Age']
    Race = df.loc[exam_index,'Race']
    Ethnicity = df.loc[exam_index,'Ethnicity']
    Density = df.loc[exam_index,'Density']
    Gender = df.loc[exam_index,'Gender']
    Procedure = df.loc[exam_index,'Procedure']
    BIRADS = df.loc[exam_index,'BI-RADS']
    Exam_Date = df.loc[exam_index,'Date']
    Exam_Laterality = df.loc[exam_index,'Laterality']
    
    left_matches = biopsy_mapping[exam_index]['LEFT']
    right_matches = biopsy_mapping[exam_index]['RIGHT']

    # refactor this code, lots of duplication in left and right
    if Exam_Laterality in ['LEFT','BILATERAL']: # loop over left matches and add one row for each match
        Biop_Laterality = 'LEFT' 
        for biop_index in left_matches:
            if biop_index < 0:
                Time_Biop = biop_index
                Biop_Accession = None
                Biop_Date = None
                Biop_Procedure = 'No Match'
                Biop_Modality = None
                Biop_Path_Note = None
                Biop_Pathology = None
            else:
                Biop_Date = df.loc[biop_index,'Date']
                Time_Biop = (Biop_Date - Exam_Date).total_seconds()/86400
                Biop_Accession = df.loc[biop_index,'Accession']
                Biop_Procedure = df.loc[biop_index,'Procedure']
                Biop_Modality = biop_modality( Biop_Procedure )
                Biop_Path_Note = df.loc[biop_index, 'Path_Note']
                Biop_Pathology = df.loc[biop_index, 'Pathology']
            new_row = {'PatientID':[PatientID],
                       'Accession':[Accession],
                       'Age':[Age],
                       'Race':[Race],
                       'Ethnicity':[Ethnicity],
                       'Density':[Density],
                       'Gender':[Gender],
                       'BI-RADS':[BIRADS],
                       'Exam_Date':[Exam_Date],
                       'Exam_Laterality':[Exam_Laterality],
                       'Time_Biop':[Time_Biop],
                       'Biop_Laterality':[Biop_Laterality], 
                       'Biop_Accession':[Biop_Accession],
                       'Biop_Date':[Biop_Date],
                       'Biop_Procedure':[Biop_Procedure],
                       'Biop_Modality':[Biop_Modality],
                       'Path_Note':[Biop_Path_Note],
                       'Pathology':[Biop_Pathology]}
            df_temp = pd.DataFrame( new_row )
            df_match = pd.concat( [df_match, df_temp] )   
    if Exam_Laterality in ['RIGHT','BILATERAL']: # loop over left matches and add one row for each match
        Biop_Laterality = 'RIGHT' 
        for biop_index in right_matches:
            if biop_index < 0:
                Time_Biop = biop_index
                Biop_Accession = None
                Biop_Date = None
                Biop_Procedure = 'No Match'
                Biop_Modality = None
                Biop_Path_Note = None
                Biop_Pathology = None
            else:
                Biop_Date = df.loc[biop_index,'Date']
                Time_Biop = (Biop_Date - Exam_Date).total_seconds()/86400
                Biop_Accession = df.loc[biop_index,'Accession']
                Biop_Procedure = df.loc[biop_index,'Procedure']
                Biop_Modality = biop_modality( Biop_Procedure )
                Biop_Path_Note = df.loc[biop_index, 'Path_Note']
                Biop_Pathology = df.loc[biop_index, 'Pathology']
            new_row = {'PatientID':[PatientID],
                       'Accession':[Accession],
                       'Age':[Age],
                       'Race':[Race],
                       'Ethnicity':[Ethnicity],
                       'Density':[Density],
                       'Gender':[Gender],
                       'BI-RADS':[BIRADS],
                       'Exam_Date':[Exam_Date],
                       'Exam_Laterality':[Exam_Laterality],
                       'Time_Biop':[Time_Biop],
                       'Biop_Laterality':[Biop_Laterality], 
                       'Biop_Accession':[Biop_Accession],
                       'Biop_Date':[Biop_Date],
                       'Biop_Procedure':[Biop_Procedure],
                       'Biop_Modality':[Biop_Modality],
                       'Path_Note':[Biop_Path_Note],
                       'Pathology':[Biop_Pathology]}
            df_temp = pd.DataFrame( new_row )
            df_match = pd.concat( [df_match, df_temp] )   



# keep only US guided biopsies and biopsies that occur 0 to 120 days after exam
df_match_us_guide = df_match[ df_match['Biop_Modality']=='US' ]
df_match_us_guide = df_match_us_guide[ df_match_us_guide['Time_Biop']>= 0] 
df_match_us_guide = df_match_us_guide.reset_index(drop=True)
df_match_us_guide.to_csv(f'{env}/input_file/US_Guided_Exams_and_Biopsies.csv',index=False)

