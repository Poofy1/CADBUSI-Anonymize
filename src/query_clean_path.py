import os
import pandas as pd
import re

# Get the current script directory and go back one directory
env = os.path.dirname(os.path.abspath(__file__))
env = os.path.dirname(env)  # Go back one directory

def check_text_for_laterality(text):
    if pd.isna(text):
        return None
    
    text = str(text).upper()
    
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

def extract_final_diagnosis(text):
    if pd.isna(text):
        return None
    
    # Use regex to find "FINAL DIAGNOSIS:" or "FINAL DIAGNOSIS"
    start_match = re.search(r'FINAL DIAGNOSIS:?', text, re.IGNORECASE)
    if not start_match:
        return None
    
    # Get the position right after the match
    start_pos = start_match.end()
    
    # Get the content after the match
    after_diagnosis = text[start_pos:].strip()
    
    # Use regex to find the next section header (uppercase word followed by a colon)
    match = re.search(r'\s([A-Z]{4,}:)', after_diagnosis)
    
    if match:
        # Get position of the next section header
        end_pos = match.start()
        # Extract text from after "FINAL DIAGNOSIS:" until the next section header
        diagnosis_text = after_diagnosis[:end_pos].strip()
        return diagnosis_text
    else:
        # If no next section header is found, return all text after "FINAL DIAGNOSIS:"
        return after_diagnosis

def extract_modality(text):
   if pd.isna(text):
       return None
   
   text = str(text).upper()
   
   # Pattern to match "MODALITY:" followed by content until the next word ending with ":"
   modality_pattern = r'MODALITY:\s*([^:]+?)(?=\s+\w+:|$)'
   
   match = re.search(modality_pattern, text)
   if match:
       return match.group(1).strip()  # Return the captured modality value
   
   return None


def categorize_pathology(text):
    if pd.isna(text):
        return "UNKNOWN"
    
    text = str(text).upper()
    
    # Malignant indicators
    malignant_terms = [
        "MALIGNANT", "CARCINOMA", "CANCER", "INVASIVE", "DCIS", "DUCTAL CARCINOMA", 
        "LOBULAR CARCINOMA", "ADENOCARCINOMA", "METASTATIC", "METASTASIS",
        "POSITIVE FOR MALIGNANCY", "HIGH GRADE DYSPLASIA"
    ]
    
    # Benign indicators
    benign_terms = [
        "BENIGN", "FIBROCYSTIC", "FIBROADENOMA", "NEGATIVE FOR MALIGNANCY", 
        "NORMAL BREAST TISSUE", "REACTIVE CHANGES", "FAT NECROSIS", 
        "USUAL DUCTAL HYPERPLASIA", "UDH", "PSEUDOANGIOMATOUS STROMAL HYPERPLASIA", 
        "PASH", "COLUMNAR CELL CHANGES", "NO EVIDENCE OF MALIGNANCY"
    ]
    
    # Concerning/indeterminate findings
    concerning_terms = [
        "ATYPICAL", "ATYPIA", "SUSPICIOUS", "INDETERMINATE", "UNCERTAIN",
        "CANNOT RULE OUT", "ADH", "ATYPICAL DUCTAL HYPERPLASIA", 
        "ALH", "ATYPICAL LOBULAR HYPERPLASIA"
    ]
    
    # Check for malignant indicators first
    for term in malignant_terms:
        if term in text:
            return "MALIGNANT"
    
    # Check for concerning/indeterminate findings
    for term in concerning_terms:
        if term in text:
            return "INDETERMINATE"
    
    # Check for benign indicators
    for term in benign_terms:
        if term in text:
            return "BENIGN"
    
    # If no clear indicators found
    return "UNKNOWN"


def filter_path_data(pathology_df):
    print("Parsing Pathology Data")
    
    # Extract laterality from PART_DESCRIPTION
    pathology_df['Pathology_Laterality'] = pathology_df['PART_DESCRIPTION'].apply(check_text_for_laterality)
    
    # Extract final diagnosis from SPECIMEN_NOTE
    pathology_df['final_diag'] = pathology_df['SPECIMEN_NOTE'].apply(extract_final_diagnosis)
    
    # simple diagnosis classification
    pathology_df['simple_diagnosis'] = pathology_df['final_diag'].apply(categorize_pathology)
    
    # Extract Modality from SPECIMEN_NOTE
    pathology_df['Modality'] = pathology_df['SPECIMEN_COMMENT'].apply(extract_modality)
    
    # Select columns for output
    columns_to_keep = [
        'PATIENT_ID', 
        'ENCOUNTER_ID', 
        'SPECIMEN_ACCESSION_NUMBER',
        'Pathology_Laterality',
        'final_diag',
        'simple_diagnosis',
        'Modality',
        'DIAGNOSIS_NAME', 
        'SPECIMEN_COMMENT',
        'SPECIMEN_PART_TYPE_NAME',
        'SPECIMEN_ACCESSION_DTM',
        'SPECIMEN_RESULT_DTM',
        'SPECIMEN_RECEIVED_DTM',
    ]
    
    # Create output dataframe with selected columns
    output_df = pathology_df[columns_to_keep].copy()
    
    # Remove duplicate rows
    rows_before = len(output_df)
    output_df = output_df.drop_duplicates(keep='first')
    rows_after = len(output_df)
    duplicates_removed = rows_before - rows_after
    
    print(f"Removed {duplicates_removed} exact duplicate rows.")
    
    # Save to CSV
    output_df.to_csv(f'{env}/raw_data/parsed_pathology.csv', index=False)
    
    return output_df