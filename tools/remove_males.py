import pandas as pd
import os
env = os.path.dirname(os.path.abspath(__file__))

# Step 1: Read file1.csv and filter rows
file1_df = pd.read_csv(f'{env}/output.csv')
filtered_file1_df = file1_df[file1_df['PatientSex'] != 'F']
patient_ids_file1 = filtered_file1_df['Patient_ID'].tolist()

# Step 2: Read file2.csv and create a mapping dictionary
file2_df = pd.read_csv(f'{env}/maps/master_anon_map.csv')
mapping_dict = dict(zip(file2_df[' AnonymizedPatientID'], file2_df[' OriginalPatientID']))
original_patient_ids = [mapping_dict[pid] for pid in patient_ids_file1 if pid in mapping_dict]

# Step 3: Read file3.csv, filter rows and save to a new file
file3_df = pd.read_csv(f'{env}/NewDataQuery.csv')
filtered_file3_df = file3_df[~file3_df['PatientID'].isin(original_patient_ids)]
filtered_file3_df.to_csv(f'{env}/NewDataQuery2.csv', index=False)
