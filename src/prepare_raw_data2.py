
import os
import pandas as pd
import re
env = os.path.dirname(os.path.abspath(__file__))

def filter_data(input_file):
    df = pd.read_csv(input_file, low_memory=False)

    rename_dict = {'PAT_PATIENT_CLINIC_NUMBER': 'Patient_ID',
        'IMGST_ACCESSION_IDENTIFIER_VALUE': 'Accession_Number',
        'IMGST_DESCRIPTION': 'Biopsy_Desc',}
    
    
    # Rename columns
    df = df.rename(columns=rename_dict)
    
    # Extract text after "DENSITY:" until next section header (WORD:)
    def extract_density(text):
        if pd.isna(text):
            return None
        
        # Check if "DENSITY:" exists in the text
        if "DENSITY:" not in text:
            return None
        
        # Split by "DENSITY:" and get the content after it
        after_density = text.split("DENSITY:")[1].strip()
        
        # Use regex to find the next uppercase word followed by a colon
        match = re.search(r'([A-Z]{2,}:)', after_density)
        
        if match:
            # Get position of the next section header
            end_pos = match.start()
            # Extract text from after "DENSITY:" until the next section header
            density_text = after_density[:end_pos].strip()
            return density_text
        else:
            # If no next section header is found, return all text after "DENSITY:"
            return after_density
    
    # Extract BI-RADS assessment value and description
    def extract_birads_and_description(text):
        if pd.isna(text):
            return None, None
        
        # Try multiple patterns to capture BI-RADS values and their descriptions
        patterns = [
            r'BI-RADS\s*ASSESSMENT:\s*(\d+[a-z]?):\s*([^\.]+)\.?',          #
            r'BI-RADS:\s*(\d+[a-z]?):\s*([^\.]+)',                           # BI-RADS: 2: Benign
            r'ASSESSMENT:\s*BI-RADS:\s*(\d+[a-z]?):\s*([^\.]+)',             # ASSESSMENT: BI-RADS: 2: Benign
            r'BI-RADS\s*(\d+[a-z]?)[:\s]+([^\.]+)',                          # More flexible pattern
            r'BI-RADS\s*Category\s*(\d+[a-z]?):\s*([^\.]+)',                 # BI-RADS Category 2: Benign Finding(s)
            r'OVERALL\s*STUDY\s*BIRADS:\s*(\d+[a-z]?)\s+([^\.]+)',           # OVERALL STUDY BIRADS: 3 Probably benign
            r'BI-RADS\s*(\d+[a-z]?),\s*([^,\.]+)',                           # BI-RADS 4, SUSPICIOUS FOR MALIGNANCY
            r'IMPRESSION:\s*BI-RADS\s*(\d+[a-z]?),\s*([^,\.]+)',             # IMPRESSION: BI-RADS 4, SUSPICIOUS FOR MALIGNANCY
            r'(?:IMPRESSION:|ASSESSMENT:)?\s*(?:BI-?RADS)\s*(\d+[a-z]?)\s*-\s*([^\.]+)', # IMPRESSION: BIRADS 2 - Benign
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match and len(match.groups()) >= 2:
                birads_category = match.group(1)
                description = match.group(2).strip()
                return birads_category, description
        
        # If we didn't find both category and description, try just getting the category
        for pattern in [
            r'BI-RADS\s*(\d+[a-z]?)', 
            r'BIRADS\s*(\d+[a-z]?)',
            r'BI-RADS\s*Category\s*(\d+[a-z]?)',
            r'OVERALL\s*STUDY\s*BIRADS:\s*(\d+[a-z]?)'
        ]: 
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                return match.group(1), None
        
        # Special case for assessment with pathology but no explicit BI-RADS
        pathology_match = re.search(r'ASSESSMENT:\s*\d+:\s*(Pathology\s+\w+)', text, re.IGNORECASE)
        if pathology_match:
            return None, pathology_match.group(1).strip()
        
        return None, None
        
    def determine_laterality(description):
        if pd.isna(description):
            return None
        
        description = description.upper()
        
        # Check for clear RIGHT indicators
        if any(x in description for x in ["RIGHT", "R BI"]) and "BILATERAL" not in description:
            return "RIGHT"
        
        # Check for clear LEFT indicators
        elif any(x in description for x in ["LEFT", "L BI"]) and "BILATERAL" not in description:
            return "LEFT"
        
        # Check for BILATERAL indicators
        elif "BILATERAL" in description:
            return "BILATERAL"
        
        # If no laterality is found, return None
        else:
            return None
    
    # Apply the extraction functions and create new columns
    df['Density_Desc'] = df['RAD_RADIOLOGY_REPORT'].apply(extract_density)
    
    # Apply the BI-RADS extraction and create separate columns
    birads_results = df['RAD_RADIOLOGY_REPORT'].apply(extract_birads_and_description)
    df['BI-RADS'] = [result[0] for result in birads_results]
    df['Biopsy'] = [result[1] for result in birads_results]
    
    # Find Laterality 
    df['Study_Laterality'] = df['Biopsy_Desc'].apply(determine_laterality)
    
    pd.set_option('display.max_colwidth', None)
    print(df.iloc[2])
    # Columns to drop
    columns_to_drop = ['RAD_RADIOLOGY_NARRATIVE', 'IMGST_PROCEDURE_CODE_TEXT', 'RAD_RADIOLOGY_REPORT', 'ENDPT_ADDRESS', 'PAT_US_CORE_BIRTHSEX', 'RAD_SERVICE_RESULT_STATUS']
    df = df.drop(columns=columns_to_drop, errors='ignore')
    
    
    
    
    
    
    df.to_csv(f'{env}/testing.csv', index=False)
    
    
    
    return df
    
    
    
    

# Execution 
input_file = f'{env}/dicom_urls.csv'
output_file = f'{env}/output.csv'

filtered_data = filter_data(input_file)
