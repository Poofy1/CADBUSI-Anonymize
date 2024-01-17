import pandas as pd
from tqdm import tqdm
import os
env = os.path.dirname(os.path.abspath(__file__))



def biop_modality( biop_description ):
    if ('ULTRASOUND') in biop_description or ('US' in biop_description):
        return 'US'
    if ('STEREO' in biop_description) or ('TOMO' in biop_description) :
        return 'X-RAY'
    if 'MR' in biop_description:
        return 'MRI'
    

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






def filter_data(input_file):
    df = pd.read_csv(input_file, low_memory=False)

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
    print("Finished Filtering Data")
    
    return df_match_us_guide





def prepare_data(filtered_data, output_file):
    
    df2 = pd.read_csv(f"{env}/maps/master_anon_map.csv")
    df3 = pd.read_csv(f"{env}/maps/master_biop_anon.csv")

    output_df = filtered_data.merge(df2[[' OriginalAccessionNumber', ' AnonymizedAccessionNumber', ' AnonymizedPatientID']], 
                        left_on='Accession', 
                        right_on=' OriginalAccessionNumber', 
                        how='left')
    output_df.drop(' OriginalAccessionNumber', axis=1, inplace=True)


    # ADD anon bio_accession
    output_df = output_df.merge(df3[['BIOP_ACCESSION', 'anon_id']], 
                        left_on='Biop_Accession', 
                        right_on='BIOP_ACCESSION', 
                        how='left')
    output_df.drop('BIOP_ACCESSION', axis=1, inplace=True)
    output_df.rename(columns={'anon_id': 'anon_biop_accession'}, inplace=True)


    # Adding missing Biopsy anon IDs
    # Get the max anon_id from df3
    max_anon_id = df3['anon_id'].max()
    # Create a mask for the rows where anon_biop_accession is NaN
    mask = output_df['anon_biop_accession'].isna()
    # For each NaN, increment max_anon_id and set the new value
    output_df.loc[mask, 'anon_biop_accession'] = range(max_anon_id + 1, max_anon_id + 1 + mask.sum())
    # Ensure the new IDs are integers
    output_df['anon_biop_accession'] = output_df['anon_biop_accession'].astype(int)
    # Create a new dataframe with the new IDs, their corresponding Biop_Accession and AnonymizedPatientID
    new_ids = output_df.loc[mask, ['PatientID', 'Biop_Accession', 'anon_biop_accession']]
    # Rename the columns to match df3
    new_ids.columns = ['PATIENTID', 'BIOP_ACCESSION', 'anon_id']
    # Append the new dataframe to df3
    df3 = df3.append(new_ids, ignore_index=True)


    # Adding missing Patient and Accession anon IDs
    # Create dictionaries from df2 for existing OriginalPatientID and OriginalAccessionNumber
    existing_patient_map = df2.set_index(' OriginalPatientID')[' AnonymizedPatientID'].to_dict()
    existing_accession_map = df2.set_index(' OriginalAccessionNumber')[' AnonymizedAccessionNumber'].to_dict()

    # Get the max AnonymizedPatientID and AnonymizedAccessionNumber from df2
    max_anon_patient_id = df2[' AnonymizedPatientID'].max()
    max_anon_accession_number = df2[' AnonymizedAccessionNumber'].max()

    # Identify the rows with missing AnonymizedPatientID and AnonymizedAccessionNumber
    missing_patients = output_df.loc[output_df[' AnonymizedPatientID'].isna(), 'PatientID'].unique()
    missing_accessions = output_df.loc[output_df[' AnonymizedAccessionNumber'].isna(), 'Accession'].unique()

    # Filter out OriginalPatientIDs and OriginalAccessionNumbers that already exist in df2
    new_missing_patients = [pid for pid in missing_patients if pid not in existing_patient_map]
    new_missing_accessions = [acc for acc in missing_accessions if acc not in existing_accession_map]

    # Create unique mappings for missing IDs, skipping the ones already in df2
    patient_id_mapping = {original: existing_patient_map.get(original, max_anon_patient_id + i + 1)
                        for i, original in enumerate(missing_patients)}
    accession_mapping = {original: existing_accession_map.get(original, max_anon_accession_number + i + 1)
                        for i, original in enumerate(missing_accessions)}

    # Apply these mappings to the dataframe
    output_df[' AnonymizedPatientID'].fillna(output_df['PatientID'].map(patient_id_mapping), inplace=True)
    output_df[' AnonymizedAccessionNumber'].fillna(output_df['Accession'].map(accession_mapping), inplace=True)

    # Ensure the new IDs are integers
    output_df[' AnonymizedPatientID'] = output_df[' AnonymizedPatientID'].astype(int)
    output_df[' AnonymizedAccessionNumber'] = output_df[' AnonymizedAccessionNumber'].astype(int)

    # Create a new dataframe with the new IDs and their corresponding PatientID and Accession
    new_ids = output_df[output_df['PatientID'].isin(new_missing_patients) | output_df['Accession'].isin(new_missing_accessions)]
    new_ids = new_ids[['PatientID', ' AnonymizedPatientID', 'Accession', ' AnonymizedAccessionNumber']].drop_duplicates()

    # Rename the columns to match df2
    new_ids.columns = [' OriginalPatientID', ' AnonymizedPatientID', ' OriginalAccessionNumber', ' AnonymizedAccessionNumber']

    # Append the new dataframe to df2
    df2 = df2.append(new_ids, ignore_index=True)



    # DATA SAFETY
    output_df.drop(['PatientID', 'Biop_Accession', 'Accession', 'Exam_Date', 'Biop_Date'], axis=1, inplace=True)


    # Renaming
    output_df = output_df.rename(columns={
        'anon_biop_accession': 'Biopsy_Accession',
        ' AnonymizedPatientID': 'Patient_ID',
        ' AnonymizedAccessionNumber': 'Accession_Number',
        'Pathology': 'Biopsy',
        'Exam_Laterality': 'Study_Laterality',
        'Density': 'Density_Desc',
        'Biop_Procedure': 'Biopsy_Desc',
        'Gender': 'PatientSex',
        'Biop_Laterality' : 'Biopsy_Laterality',
        'Path_Note' : 'Path_Desc'
    })
    
    df2.to_csv(f"{env}/maps/master_anon_map2.csv", index=False)
    df3.to_csv(f"{env}/maps/master_biop_anon2.csv", index=False)
    output_df.to_csv(output_file, index=False)
    
    print("Finished Output")








# Execution 
input_file = f'{env}/input_data/data_complete.csv'
output_file = f'{env}/output/total_cases_anon2.csv'


filtered_data = filter_data(input_file)
filtered_data.to_csv(f'{env}/input_data/filtered_data.csv')


prepare_data(filtered_data, output_file)