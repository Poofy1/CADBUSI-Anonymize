import pandas as pd
import os
env = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Read the CSV files
us_guided = pd.read_csv(f'{env}/input_file/US_Guided_Exams_and_Biopsies.csv')
master_map = pd.read_csv(f'{env}/maps/master_anon_map.csv')

# Determine which accessions in the 'us_guided' dataframe are not present in the 'master_map' dataframe
missing_accessions = us_guided[~us_guided['Accession'].isin(master_map[' OriginalAccessionNumber'])]

# Display or save the missing accessions
if missing_accessions.empty:
    print("All accessions in US_Guided_Exams_and_Biopsies are present in master_anon_map.")
else:
    print("Missing Accessions:")
    print(missing_accessions['Accession'])