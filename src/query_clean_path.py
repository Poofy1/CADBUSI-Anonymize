import os
import pandas as pd
import re

# Get the current script directory and go back one directory
env = os.path.dirname(os.path.abspath(__file__))
env = os.path.dirname(env)  # Go back one directory

def determine_laterality(row):
    """Determine laterality from pathology report, with improved handling of multi-part reports."""
    
    def check_text_for_laterality(text):
        if pd.isna(text):
            return None
        
        text = text.upper()
        
        # Track mentions of each side in multi-part reports
        right_mentions = 0
        left_mentions = 0
        
        # Handle multi-part reports (A, B, C sections)
        parts = re.split(r'(?:PART |^)([A-Z])\.\s+', text)
        
        # If no parts found, check the whole text
        if len(parts) <= 1:
            if "RIGHT" in text and "LEFT" in text:
                return "BILATERAL"
            elif "RIGHT" in text and "BILATERAL" not in text:
                return "RIGHT"
            elif "LEFT" in text and "BILATERAL" not in text:
                return "LEFT"
            elif "BILATERAL" in text:
                return "BILATERAL"
        else:
            # Process each part separately
            for i in range(1, len(parts), 2):
                if i+1 < len(parts):
                    part_text = parts[i+1]
                    if "RIGHT" in part_text:
                        right_mentions += 1
                    if "LEFT" in part_text:
                        left_mentions += 1
            
            # Determine overall laterality based on part counts
            if right_mentions > 0 and left_mentions > 0:
                return "BILATERAL"
            elif right_mentions > 0:
                return "RIGHT"
            elif left_mentions > 0:
                return "LEFT"
        
        # Check for explicit BILATERAL indicator if no parts found
        if "BILATERAL" in text:
            return "BILATERAL"
        
        # If no laterality is found
        return None
    
    # First try final_diag column if it exists
    if 'final_diag' in row and not pd.isna(row['final_diag']):
        laterality = check_text_for_laterality(row['final_diag'])
        if laterality is not None:
            return laterality
    
    # Then try PART_DESCRIPTION column
    if 'PART_DESCRIPTION' in row and not pd.isna(row['PART_DESCRIPTION']):
        laterality = check_text_for_laterality(row['PART_DESCRIPTION'])
        if laterality is not None:
            return laterality
    
    # If not found or previous columns are empty, try SPECIMEN_NOTE
    if 'SPECIMEN_NOTE' in row and not pd.isna(row['SPECIMEN_NOTE']):
        laterality = check_text_for_laterality(row['SPECIMEN_NOTE'])
        if laterality is not None:
            return laterality
    
    # If still not found, return None
    return None


def split_bilateral_cases(pathology_df):
    """
    Split bilateral pathology cases into separate rows for left and right sides.
    """
    print("Splitting bilateral cases into separate rows...")
    expanded_rows = []
    bilateral_count = 0
    
    for idx, row in pathology_df.iterrows():
        if pd.isna(row['final_diag']):
            # Keep the original row for cases with no final diagnosis
            expanded_rows.append(row.to_dict())
            continue
        
        text = str(row['final_diag']).upper()
        
        # Check if this is a multi-part report with both LEFT and RIGHT
        if "LEFT" in text and "RIGHT" in text:
            bilateral_count += 1
            
            # Split the text into parts
            parts = re.split(r'([A-Z])\.\s+', text)
            
            left_parts = []
            right_parts = []
            
            # Separate parts by laterality
            for i in range(1, len(parts), 2):
                if i+1 < len(parts):
                    part_letter = parts[i]
                    part_text = parts[i+1]
                    
                    if "LEFT" in part_text:
                        left_parts.append(f"{part_letter}. {part_text}")
                    elif "RIGHT" in part_text:
                        right_parts.append(f"{part_letter}. {part_text}")
            
            # Create a row for the LEFT side if parts exist
            if left_parts:
                left_row = row.to_dict()
                left_row['final_diag'] = " ".join(left_parts)
                left_row['Pathology_Laterality'] = "LEFT"
                expanded_rows.append(left_row)
            
            # Create a row for the RIGHT side if parts exist
            if right_parts:
                right_row = row.to_dict()
                right_row['final_diag'] = " ".join(right_parts)
                right_row['Pathology_Laterality'] = "RIGHT"
                expanded_rows.append(right_row)
        else:
            # Just add the original row for non-bilateral cases
            expanded_rows.append(row.to_dict())
    
    # Create a new dataframe from the expanded rows
    expanded_df = pd.DataFrame(expanded_rows)
    
    print(f"Split {bilateral_count} bilateral cases into separate rows.")
    return expanded_df


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
    
    # Important: First check for explicit malignant diagnoses
    explicit_malignant_patterns = [
        r"INVASIVE\s+DUCTAL\s+CARCINOMA", 
        r"DUCTAL\s+CARCINOMA\s+IN\s+SITU", 
        r"DCIS",
        r"METASTATIC\s+CARCINOMA",
        r"POSITIVE\s+FOR\s+CARCINOMA",
        r"INVASIVE\s+CARCINOMA"
    ]
    
    for pattern in explicit_malignant_patterns:
        if re.search(pattern, text):
            return "MALIGNANT"
    
    # Check for explicit negation patterns - expanded to catch truncated words
    negation_patterns = [
        r"NEGATIVE\s+FOR\s+MALIGNAN[CT]", 
        r"NO\s+EVIDENCE\s+OF\s+MALIGNAN[CT]",
        r"NEGATIVE\s+FOR\s+ATYPIA\s+AND\s+CARCINOMA",
        r"NO\s+EVIDENCE\s+OF\s+ATYPIA\s+OR\s+MALIGNAN[CT]",
        r"NO\s+EVIDENCE\s+OF\s+ATYPIA\s+AND\s+MALIGNAN[CT]",
        r"THERE\s+IS\s+NO\s+EVIDENCE\s+OF\s+ATYPIA\s+OR\s+MALIGNAN[CT]",
        r"LYMPH\s+NODES?\s+NEGATIVE\s+FOR\s+MALIGNAN[CT]",
        r"NEGATIVE\s+FOR\s+TUM[O|OR]",
        r"NO\s+MALIGNANCY\s+PRESENT"
    ]
    
    # Only consider a negation pattern if it refers to the entire sample
    for pattern in negation_patterns:
        if re.search(pattern, text):
            return "BENIGN"
    
    # Malignant indicators - clearly cancer/malignancy
    malignant_terms = [
        r"MALIGNANT", r"CARCINOMA", r"CANCER", r"INVASIVE", r"DCIS",
        r"DUCTAL\s+CARCINOMA", r"LOBULAR\s+CARCINOMA", r"ADENOCARCINOMA", 
        r"METASTATIC", r"METASTASIS", r"POSITIVE\s+FOR\s+MALIGNANCY", 
        r"HIGH\s+GRADE\s+DYSPLASIA", r"LYMPHOVASCULAR\s+INVASION"
    ]
    
    # Check if any malignant term is present (not negated)
    for term in malignant_terms:
        if re.search(term, text):
            term_match = re.search(term, text)
            if term_match:
                term_pos = term_match.start()
                start_window = max(0, term_pos - 20)
                context = text[start_window:term_pos + len(term_match.group()) + 5]
                if not re.search(r"NO\s+" + term, context) and not re.search(r"NEGATIVE\s+FOR\s+" + term, context):
                    return "MALIGNANT"
    
    # Check for implant/expander-related cases specifically
    implant_patterns = [
        r"EXPLANTED\s+(?:LEFT|RIGHT)?\s*BREAST\s+(?:TISSUE\s+)?(?:IMPLANT|EXPANDER)",
        r"BREAST\s+IMPLANT\s+CAPSUL",
        r"CONSISTENT\s+WITH\s+BREAST\s+IMPLANT\s+CAPSUL",
        r"GROSS\s+ONLY[,\s]*AS\s+DESCRIBE",
        r"BREAST\s+IMPLANT\s+IDENTIFIE[D]*",
        r"BREAST\s+CAPSULE[,\s].*CAPSULECTOMY",
        r"FIBROUS\s+CAPSULE\s+WITH\s+(?:FOREIGN\s+BODY[^\w]+TYPE)?\s*GRANULOMATOUS\s+REACTIO[N]*"
    ]
    
    implant_match = False
    for pattern in implant_patterns:
        if re.search(pattern, text):
            implant_match = True
            break
    
    # Enhanced benign indicators - additional terms for common benign findings
    benign_terms = [
        # Original benign terms with flexible matching
        r"FIBRO(?:CYSTIC|ADENO(?:M|MA))", r"INTRADUCTAL\s+PAPILLOMA", r"FIBROMATOSIS",
        r"NORMAL\s+BREAST\s+TISSUE", r"REACTIVE\s+CHANGES", r"FAT\s+NECROSIS", 
        r"USUAL\s+DUCTAL\s+HYPERPLAS[I|IA]", r"UDH", r"PSEUDOANGIOMATOUS\s+STROMAL\s+HYPERPLAS[I|IA]", 
        r"PASH", r"COLUMNAR\s+CELL\s+CHANGES", r"NONPROLIFERATIVE\s+FIBROCYSTIC\s+CHANGE",
        r"NODULAR\s+ADENOSIS", r"APOCRINE\s+METAPLAS[I|IA]", r"USUAL\s+TYPE\s+HYPERPLAS[I|IA]",
        r"ADENOS[I|IS]",
        r"FIBROADIPOSE\s+TISSUE(?:\s+WITH\s+SCARRING)?",
        r"SKIN\s+WITH\s+SCARRING",
        r"(?:DENSE)?\s*FIBROUS\s+TISSUE",
        r"FOAMY\s+HISTIOCYTES",
        r"GIANT\s+CELLS",
        r"REDUCTION\s+MAMMOPLASTY",
        r"FIBROSIS",
        
        # New terms for inflammatory and benign conditions
        r"CHRONIC\s+(?:AND\s+)?GRANULOMATOUS\s+INFLAMMATION",
        r"FOREIGN\s+BODY\s+GIANT\s+CELL\s+REACTION",
        r"CYSTICALLY\s+DILATED\s+SQUAMOUS\s+LINED\s+DUCT",
        r"GRANULOMATOUS\s+INFLAMMATION",
        r"FOREIGN\s+BODY[^\w]+TYPE\s+GRANULOMATOUS\s+REACTIO[N]*",
        
        # Biopsy site changes and calcifications
        r"CHANGES\s+COMPATIBLE\s+WITH\s+PRIOR\s+PROCEDURE\s+SITE",
        r"DYSTROPHIC\s+CALCIFICATIONS?",
        r"BIOPSY\s+SITE\s+CHANGES",
        r"PRIOR\s+BIOPSY\s+SITE",
        r"POST[\s\-]PROCEDURAL\s+CHANGES",
        
        # Uncertain/non-diagnostic findings (treat as benign if no clear malignancy)
        r"PAPILLARY\s+LESION\s+CAN\s+NOT\s+BE\s+EXCLUDED",
        r"SUGGESTING\s+PAPILLARY\s+LESION",
        r"CLINICAL\s+AND\s+RADIOLOGIC\s+CORRELATION\s+IS\s+RECOMMENDED",
        r"DOES\s+NOT\s+REVEAL\s+A\s+SPECIFIC\s+LESION",
        r"SPARSE\s+BREAST\s+TISSUE",
        r"SUGGESTING\s+POSSIBLE\s+DISRUPTION",
        
        # Former "indeterminate" findings now classified as benign
        r"ATYPICAL", r"ATYPIA", r"ADH", r"ATYPICAL\s+DUCTAL\s+HYPERPLAS[I|IA]", 
        r"ALH", r"ATYPICAL\s+LOBULAR\s+HYPERPLAS[I|IA]"
    ]
    
    # Check for benign indicators
    for term in benign_terms:
        if re.search(term, text):
            return "BENIGN"
    
    # If we matched an implant pattern but no specific benign or malignant findings,
    # classify as BENIGN since these are generally non-pathological specimens
    if implant_match:
        return "BENIGN"
    
    # Additional benign indicators or explicit "BENIGN" statement
    if "BENIGN" in text:
        return "BENIGN"
    
    # Some potentially concerning findings that aren't clearly benign 
    # but also not malignant - classifying as BENIGN
    less_concerning_terms = [
        r"SUSPICIOUS", r"INDETERMINATE", r"UNCERTAIN",
        r"CANNOT\s+RULE\s+OUT", r"CAN\s+NOT\s+BE\s+EXCLUDED"
    ]
    
    for term in less_concerning_terms:
        pattern = term
        if re.search(pattern, text) and not re.search(r"NEGATIVE\s+FOR\s+" + pattern, text) and not re.search(r"NO\s+EVIDENCE\s+OF\s+" + pattern, text):
            return "BENIGN"
    
    # Default to BENIGN for mastectomy, excision, biopsy, capsulectomy, and removal specimens 
    # without malignant findings - this is a conservative assumption
    if re.search(r"(MASTECTOMY|NIPPLE-SPARING\s+MASTECTOMY|EXCISION|BIOPSY|CAPSULECTOMY|REMOVAL)", text) and not re.search(r"MALIGNAN[CT]|CARCINOMA|CANCER", text):
        return "BENIGN"
    
    # If no clear indicators found
    return "UNKNOWN"

def filter_path_data(pathology_df):
    print("Parsing Pathology Data")
    
    # Extract final diagnosis from SPECIMEN_NOTE
    pathology_df['final_diag'] = pathology_df['SPECIMEN_NOTE'].apply(extract_final_diagnosis)
    
    # Split bilateral cases into separate rows
    expanded_df = split_bilateral_cases(pathology_df)
    
    # Re-determine laterality after splitting (for rows that didn't have it set during splitting)
    expanded_df['Pathology_Laterality'] = expanded_df.apply(determine_laterality, axis=1)
    
    # Apply diagnosis classification
    expanded_df['path_interpretation'] = expanded_df['final_diag'].apply(categorize_pathology)
    
    # Extract Modality from SPECIMEN_COMMENT
    expanded_df['Modality'] = expanded_df['SPECIMEN_COMMENT'].apply(extract_modality)
    
    # Select columns for output
    columns_to_keep = [
        'PATIENT_ID', 
        'ENCOUNTER_ID', 
        'SPECIMEN_ACCESSION_NUMBER',
        'Pathology_Laterality',
        'final_diag',
        'path_interpretation',
        'Modality',
        'DIAGNOSIS_NAME', 
        'SPECIMEN_COMMENT',
        'SPECIMEN_PART_TYPE_NAME',
        'SPECIMEN_ACCESSION_DTM',
        'SPECIMEN_RESULT_DTM',
        'SPECIMEN_RECEIVED_DTM',
    ]
    
    # Create output dataframe with selected columns
    output_df = expanded_df[columns_to_keep].copy()
    
    # Remove duplicate rows
    rows_before = len(output_df)
    output_df = output_df.drop_duplicates(keep='first')
    rows_after = len(output_df)
    duplicates_removed = rows_before - rows_after
    
    print(f"Removed {duplicates_removed} exact duplicate rows.")
    
    # Save to CSV
    output_df.to_csv(f'{env}/raw_data/parsed_pathology.csv', index=False)
    
    return output_df

if __name__ == "__main__":
    pathology_df = pd.read_csv(f'{env}/raw_data/raw_pathology.csv')
    filter_path_data(pathology_df)