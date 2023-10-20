import os
import pandas as pd
env = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# ADD AnonymizedAccessionNumber / AnonymizedPatientID
df1 = pd.read_csv(f"{env}/input_file/US_Guided_Exams_and_Biopsies.csv")
df2 = pd.read_csv(f"{env}/maps_old/master_anon_map.csv")
df3 = pd.read_csv(f"{env}/maps_old/master_biop_anon.csv")


output_df = df1.merge(df2[[' OriginalAccessionNumber', ' AnonymizedAccessionNumber', ' AnonymizedPatientID']], 
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

# OUT
df2.to_csv(f"{env}/maps/master_anon_map.csv", index=False)
df3.to_csv(f"{env}/maps/master_biop_anon.csv", index=False)
output_df.to_csv(f"{env}/output.csv", index=False)