from google.cloud import bigquery
import time

def get_patient_and_accession_ids(limit=None):
    """
    Get all relevant patient IDs and accession numbers for breast imaging studies
    
    Args:
        limit (int, optional): Number of results to limit the query to.
    
    Returns:
        pandas.DataFrame: Query results as a dataframe
    """
    start_time = time.time()
    print("Initializing BigQuery client...")
    client = bigquery.Client()
    
    query = """
    WITH filtered_imaging_studies AS (
      SELECT *
      FROM `ml-mps-adl-intfhr-phi-p-3b6e.phi_secondary_use_fhir_clinicnumber_us_p.ImagingStudy`
      WHERE procedure_code_coding_code IN ('IMG3247','IMG4159','IMG4009','IMG1073','IMG4225','IMG3211',
                                           'IMG3249','IMG3248','IMG3241','IMG3330','IMG3329','IMG1100',
                                           'IMG3265','IMG3240','IMG3246','IMG3509','IMG3245','IMG3508',
                                           'IMG3326')
    )
    SELECT DISTINCT 
      PAT_PATIENT.CLINIC_NUMBER AS PATIENT_ID,
      filtered_imaging_studies.ACCESSION_IDENTIFIER_VALUE AS ACCESSION_NUMBER
    FROM filtered_imaging_studies
    INNER JOIN 
      `ml-mps-adl-intfhr-phi-p-3b6e.phi_secondary_use_fhir_clinicnumber_us_p.Patient` PAT_PATIENT 
      ON (filtered_imaging_studies.clinic_number = PAT_PATIENT.clinic_number)
    """
    
    if limit is not None:
        query += f"\nLIMIT {limit}"
        print(f"Running patient and accession ID query with limit of {limit} rows...")
    else:
        print("Running patient and accession ID query without row limit...")
    
    query_start_time = time.time()
    print("Executing query...")
    df = client.query(query).to_dataframe()
    query_end_time = time.time()
    query_duration = query_end_time - query_start_time
    
    print(f"Patient and accession ID query complete. Retrieved {len(df)} rows in {query_duration:.2f} seconds.")
    
    return df

def get_radiology_data(accession_numbers):
    """
    Get radiology data for specific accession numbers
    
    Args:
        accession_numbers (list): List of accession numbers to query
    
    Returns:
        pandas.DataFrame: Query results as a dataframe
    """
    start_time = time.time()
    print("Initializing BigQuery client for radiology data...")
    client = bigquery.Client()
    
    # Format accession numbers (these are typically strings)
    accession_str = ', '.join([f"'{acc}'" for acc in accession_numbers])
    
    query = f"""
    SELECT DISTINCT 
      PAT_PATIENT.CLINIC_NUMBER AS PATIENT_ID,
      filtered_imaging_studies.ACCESSION_IDENTIFIER_VALUE AS ACCESSION_NUMBER,
      filtered_imaging_studies.DESCRIPTION,
      filtered_imaging_studies.PROCEDURE_CODE_TEXT,
      ENDPOINT.ADDRESS AS ENDPOINT_ADDRESS,
      PAT_PATIENT.US_CORE_BIRTHSEX,
      RAD_FACT_RADIOLOGY.RADIOLOGY_NARRATIVE,
      RAD_FACT_RADIOLOGY.RADIOLOGY_REPORT,
      RAD_FACT_RADIOLOGY.SERVICE_RESULT_STATUS,
      RAD_FACT_RADIOLOGY.RADIOLOGY_DTM,
      RAD_FACT_RADIOLOGY.RADIOLOGY_REVIEW_DTM
    FROM `ml-mps-adl-intfhr-phi-p-3b6e.phi_secondary_use_fhir_clinicnumber_us_p.ImagingStudy` filtered_imaging_studies
    INNER JOIN 
      `ml-mps-adl-intfhr-phi-p-3b6e.phi_secondary_use_fhir_clinicnumber_us_p.Patient` PAT_PATIENT 
      ON (filtered_imaging_studies.clinic_number = PAT_PATIENT.clinic_number)
    INNER JOIN 
      `ml-mps-adl-intfhr-phi-p-3b6e.phi_secondary_use_fhir_clinicnumber_us_p.Endpoint` ENDPOINT 
      ON (filtered_imaging_studies.gcp_endpoint_id = ENDPOINT.id)
    LEFT JOIN 
      `ml-mps-adl-intudp-phi-p-d5cb.phi_udpwh_etl_us_p.FACT_RADIOLOGY` RAD_FACT_RADIOLOGY 
      ON (filtered_imaging_studies.ACCESSION_IDENTIFIER_VALUE = RAD_FACT_RADIOLOGY.ACCESSION_NBR)
    WHERE filtered_imaging_studies.ACCESSION_IDENTIFIER_VALUE IN ({accession_str})
    """

    query_start_time = time.time()
    print("Executing radiology query...")
    df = client.query(query).to_dataframe()
    query_end_time = time.time()
    query_duration = query_end_time - query_start_time
    
    print(f"Radiology query complete. Retrieved {len(df)} rows in {query_duration:.2f} seconds.")
    
    return df

def get_pathology_data(patient_ids):
    """
    Get pathology data for specific patient IDs
    
    Args:
        patient_ids (list): List of patient IDs to query
    
    Returns:
        pandas.DataFrame: Query results as a dataframe
    """
    start_time = time.time()
    print("Initializing BigQuery client for pathology data...")
    client = bigquery.Client()
    
    # Check if patient IDs are numeric and format accordingly to match column type
    if patient_ids and all(str(id).isdigit() for id in patient_ids):
        # Format as numbers without quotes for INT64 column
        ids_str = ', '.join([str(id) for id in patient_ids])
    else:
        # Format as quoted strings for STRING column
        ids_str = ', '.join([f"'{id}'" for id in patient_ids])
    
    query = f"""
    SELECT 
      PAT_DIM_PATIENT.PATIENT_CLINIC_NUMBER AS PATIENT_ID,
      PATH_FACT_PATHOLOGY.SPECIMEN_NOTE,
      PATH_FACT_PATHOLOGY.SPECIMEN_UPDATE_DTM,
      PATH_FACT_PATHOLOGY.SPECIMEN_RESULT_DTM,
      PATH_FACT_PATHOLOGY.SPECIMEN_RECEIVED_DTM,
      PATH_FACT_PATHOLOGY.SPECIMEN_SERVICE_DESCRIPTION,
      PATH_FACT_PATHOLOGY.ENCOUNTER_ID,
      DIAGCODE_DIM_DIAGNOSIS_CODE.DIAGNOSIS_NAME,
      PATH_FACT_PATHOLOGY.PATHOLOGY_COUNT,
      PATH_FACT_PATHOLOGY.SPECIMEN_COMMENT,
      PATH_FACT_PATHOLOGY.SPECIMEN_ACCESSION_DTM,
      PATH_FACT_PATHOLOGY.SPECIMEN_ACCESSION_NUMBER,
      SPECDET.PART_DESCRIPTION,
      SPECPARTYP.SPECIMEN_PART_TYPE_CODE,
      SPECPARTYP.SPECIMEN_PART_TYPE_NAME
    FROM `ml-mps-adl-intudp-phi-p-d5cb.phi_udpwh_etl_us_p.FACT_PATHOLOGY` PATH_FACT_PATHOLOGY
    INNER JOIN
      `ml-mps-adl-intudp-phi-p-d5cb.phi_udpwh_etl_us_p.DIM_PATIENT` PAT_DIM_PATIENT
      ON (PATH_FACT_PATHOLOGY.PATIENT_DK = PAT_DIM_PATIENT.PATIENT_DK)
    LEFT JOIN
      `ml-mps-adl-intudp-phi-p-d5cb.phi_udpwh_etl_us_p.DIM_PATHOLOGY_DIAGNOSIS_CODE_BRIDGE` PATHDIAG
      ON (PATH_FACT_PATHOLOGY.PATHOLOGY_FPK = PATHDIAG.PATHOLOGY_FPK)
    LEFT JOIN
      `ml-mps-adl-intudp-phi-p-d5cb.phi_udpwh_etl_us_p.DIM_DIAGNOSIS_CODE` DIAGCODE_DIM_DIAGNOSIS_CODE
      ON (PATHDIAG.DIAGNOSIS_CODE_DK = DIAGCODE_DIM_DIAGNOSIS_CODE.DIAGNOSIS_CODE_DK)
    LEFT JOIN
      `ml-mps-adl-intudp-phi-p-d5cb.phi_udpwh_etl_us_p.FACT_PATHOLOGY_SPECIMEN_DETAIL` SPECDET
      ON (PATH_FACT_PATHOLOGY.PATHOLOGY_FPK = SPECDET.PATHOLOGY_FPK)
    LEFT JOIN
      `ml-mps-adl-intudp-phi-p-d5cb.phi_udpwh_etl_us_p.DIM_SPECIMEN_PART_TYPE` SPECPARTYP
      ON (SPECDET.SPECIMEN_PART_TYPE_DK = SPECPARTYP.SPECIMEN_PART_TYPE_DK)
    WHERE PAT_DIM_PATIENT.PATIENT_CLINIC_NUMBER IN ({ids_str})
    AND (
      LOWER(SPECPARTYP.SPECIMEN_PART_TYPE_CODE) IN ('breast','breast1','breast2','breast3','breast4','breast5','breast6','breast7','breast8','breast9','breast10','breast11')
    )
    """
    
    query_start_time = time.time()
    print("Executing pathology query...")
    df = client.query(query).to_dataframe()
    query_end_time = time.time()
    query_duration = query_end_time - query_start_time
    
    print(f"Pathology query complete. Retrieved {len(df)} rows in {query_duration:.2f} seconds.")
    
    return df

def run_breast_imaging_query(limit=None):
    """
    Run all queries and save results to CSV files
    
    Args:
        patient_limit (int, optional): Limit for patient ID query
        radiology_limit (int, optional): Limit for radiology data query
        pathology_limit (int, optional): Limit for pathology data query
    """
    total_start_time = time.time()
    print("Starting breast imaging query process...")
    
    # Step 1: Get patient IDs and accession numbers
    print("\n=== PATIENT AND ACCESSION ID QUERY ===")
    data_df = get_patient_and_accession_ids(limit=limit)
    patient_ids = data_df['PATIENT_ID'].unique().tolist()
    accession_numbers = data_df['ACCESSION_NUMBER'].tolist()
    data_df.to_csv('patient_accession_ids.csv', index=False)
    print(f"Patient and accession IDs saved to patient_accession_ids.csv")
    
    # Step 2: Get radiology data using accession numbers
    print("\n=== RADIOLOGY DATA QUERY ===")
    rad_df = get_radiology_data(accession_numbers)
    rad_df.to_csv('radiology_data.csv', index=False)
    print(f"Radiology data saved to radiology_data.csv")
    
    # Step 3: Get pathology data
    print("\n=== PATHOLOGY DATA QUERY ===")
    path_df = get_pathology_data(patient_ids)
    path_df.to_csv('pathology_data.csv', index=False)
    print(f"Pathology data saved to pathology_data.csv")
    
    total_end_time = time.time()
    total_duration = total_end_time - total_start_time
    print(f"\nAll queries complete! Total execution time: {total_duration:.2f} seconds")