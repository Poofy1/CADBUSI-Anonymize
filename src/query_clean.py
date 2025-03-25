
import os
import pandas as pd
import re
# Get the current script directory and go back one directory
env = os.path.dirname(os.path.abspath(__file__))
env = os.path.dirname(env)  # Go back one directory



def determine_laterality(row):
    # Function to check a single text field
    def check_text_for_laterality(text):
        if pd.isna(text):
            return None
        
        text = text.upper()
        
        # Check for clear RIGHT indicators
        if any(x in text for x in ["RIGHT", "R BI"]) and "BILATERAL" not in text:
            return "RIGHT"
        
        # Check for clear LEFT indicators
        elif any(x in text for x in ["LEFT", "L BI"]) and "BILATERAL" not in text:
            return "LEFT"
        
        # Check for BILATERAL indicators
        elif "BILATERAL" in text:
            return "BILATERAL"
        
        # If no laterality is found, return None
        else:
            return None
    
    # First try DESCRIPTION column
    if 'DESCRIPTION' in row and not pd.isna(row['DESCRIPTION']):
        laterality = check_text_for_laterality(row['DESCRIPTION'])
        if laterality is not None:
            return laterality
    
    # If not found or DESCRIPTION is empty, try RADIOLOGY_REPORT
    if 'RADIOLOGY_REPORT' in row and not pd.isna(row['RADIOLOGY_REPORT']):
        laterality = check_text_for_laterality(row['RADIOLOGY_REPORT'])
        if laterality is not None:
            return laterality
    
    # If still not found, return None
    return None


def extract_birads_and_description(text):
    if pd.isna(text):
        return None, None
    
    # List of keywords that should end a description
    end_keywords = [
        'benign', 'malignant', 'malignancy', 'suspicious', 
        'negative', 'positive', 'cancer', 'indeterminate', 'incomplete'
    ]
    
    # Create pattern to find any of the keywords
    end_pattern = r'(?i)(' + '|'.join(end_keywords) + r')[^\w]'
    
    # Try multiple patterns to capture BI-RADS values and their descriptions
    patterns = [
        r'BI-RADS\s*ASSESSMENT:\s*(\d+[a-z]?):\s*([^\.]+)\.?',
        r'BI-RADS:\s*(\d+[a-z]?):\s*([^\.]+)',
        r'ASSESSMENT:\s*BI-RADS:\s*(\d+[a-z]?):\s*([^\.]+)',
        r'BI-RADS\s*(\d+[a-z]?)[:\s]+([^\.]+)',
        r'BI-RADS\s*Category\s*(\d+[a-z]?):\s*([^\.]+)',
        r'OVERALL\s*STUDY\s*BIRADS:\s*(\d+[a-z]?)\s+([^\.]+)',
        r'BI-RADS\s*(\d+[a-z]?),\s*([^,\.]+)',
        r'IMPRESSION:\s*BI-RADS\s*(\d+[a-z]?),\s*([^,\.]+)',
        r'(?:IMPRESSION:|ASSESSMENT:)?\s*(?:BI-?RADS)\s*(\d+[a-z]?)\s*-\s*([^\.]+)',
    ]
    
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match and len(match.groups()) >= 2:
            birads_category = match.group(1)
            full_description = match.group(2).strip()
            
            # Truncate description at any of the specified keywords
            keyword_match = re.search(end_pattern, full_description + ' ')
            if keyword_match:
                # Get the position of the keyword plus its length
                key_end_pos = keyword_match.end() - 1  # -1 to exclude the non-word character
                description = full_description[:key_end_pos].strip()
            else:
                description = full_description
                
            return birads_category, description
    
    # Rest of the function remains the same
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


def extract_density(text):
    # Extract text after "DENSITY:" until next section header (WORD:)
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


def extract_rad_pathology_txt(text):
    if pd.isna(text):
        return None
    
    # Check if "PATHOLOGY:" exists in the text
    if "PATHOLOGY:" not in text:
        return None
    
    # Split by "PATHOLOGY:" and get the content after it
    after_pathology = text.split("PATHOLOGY:")[1].strip()
    
    # Use regex to find the next uppercase word followed by a colon
    match = re.search(r'([A-Z]{2,}:)', after_pathology)
    
    if match:
        # Get position of the next section header
        end_pos = match.start()
        # Extract text from after "PATHOLOGY:" until the next section header
        pathology_text = after_pathology[:end_pos].strip()
        return pathology_text
    else:
        # If no next section header is found, return all text after "PATHOLOGY:"
        return after_pathology
    
    
def filter_data(radiology_csv, pathology_csv):
    radiology_df = pd.read_csv(radiology_csv, low_memory=False)
    pathology_df = pd.read_csv(pathology_csv, low_memory=False)

    rename_dict = {'PAT_PATIENT_CLINIC_NUMBER': 'Patient_ID',
        'IMGST_ACCESSION_IDENTIFIER_VALUE': 'Accession_Number',
        'IMGST_DESCRIPTION': 'Biopsy_Desc',}
    
    # Rename columns
    radiology_df = radiology_df.rename(columns=rename_dict)
    
    # Apply the extraction functions and create new columns
    radiology_df['Density_Desc'] = radiology_df['RADIOLOGY_REPORT'].apply(extract_density)
    
    # Apply the BI-RADS extraction and create separate columns
    birads_results = radiology_df['RADIOLOGY_REPORT'].apply(extract_birads_and_description)
    radiology_df['BI-RADS'] = [result[0] for result in birads_results]
    radiology_df['Biopsy'] = [result[1] for result in birads_results]
    
    # Find Laterality 
    radiology_df['Study_Laterality'] = radiology_df.apply(determine_laterality, axis=1)
    
    # Extract pathology text
    radiology_df['rad_pathology_txt'] = radiology_df['RADIOLOGY_REPORT'].apply(extract_rad_pathology_txt)
        
    pd.set_option('display.max_colwidth', None)
    # Columns to drop
    columns_to_drop = ['RADIOLOGY_NARRATIVE', 'PROCEDURE_CODE_TEXT', 'RADIOLOGY_REPORT', 'RAD_SERVICE_RESULT_STATUS']
    radiology_df = radiology_df.drop(columns=columns_to_drop, errors='ignore')
    
    radiology_df.to_csv(f'{env}/output.csv', index=False)
    
    
    
    

# Execution 
radiology_csv = f'{env}/radiology_data.csv'
pathology_csv = f'{env}/pathology_data.csv'

filter_data(radiology_csv, pathology_csv)
