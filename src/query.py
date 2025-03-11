from google.cloud import bigquery
import time

def run_breast_imaging_query(limit=None):
    """
    Run query for breast imaging studies
    
    Args:
        limit (int, optional): Number of results to limit the query to.
                              If None, no limit is applied.
    
    Returns:
        pandas.DataFrame: Query results as a dataframe
    """
    start_time = time.time()
    print("Initializing BigQuery client...")
    client = bigquery.Client()
    
    # Build query
    query = """
    WITH filtered_imaging_studies AS (
      SELECT *
      FROM `ml-mps-adl-intfhr-phi-p-3b6e.phi_secondary_use_fhir_clinicnumber_us_p.ImagingStudy`
      WHERE procedure_code_coding_code IN ('IMG1100','IMG3245','IMG3246','IMG3376','IMG3375','IMG3371',
                                          'IMG3251','IMG3252','IMG3576','IMG3577','IMG10902','IMG3388',
                                          'IMG3387','IMG3255','IMG3256','IMG10833','IMG3229','IMG3240',
                                          'IMG3265','IMG3315','IMG3233','IMG3341','IMG3342','IMG3326',
                                          'IMG3329','IMG3330','IMG3241','IMG3248','IMG3249','IMG3231')
    )

    SELECT DISTINCT 
      PAT_PATIENT.CLINIC_NUMBER AS PAT_PATIENT_CLINIC_NUMBER,
      filtered_imaging_studies.ACCESSION_IDENTIFIER_VALUE AS IMGST_ACCESSION_IDENTIFIER_VALUE,
      ENDPOINT.ADDRESS AS ENDPT_ADDRESS,
      filtered_imaging_studies.PROCEDURE_CODE_TEXT AS IMGST_PROCEDURE_CODE_TEXT,
      PAT_PATIENT.US_CORE_BIRTHSEX AS PAT_US_CORE_BIRTHSEX,
      filtered_imaging_studies.DESCRIPTION AS IMGST_DESCRIPTION,
      RAD_FACT_RADIOLOGY.RADIOLOGY_NARRATIVE AS RAD_RADIOLOGY_NARRATIVE,
      RAD_FACT_RADIOLOGY.RADIOLOGY_REPORT AS RAD_RADIOLOGY_REPORT,
      RAD_FACT_RADIOLOGY.SERVICE_RESULT_STATUS AS RAD_SERVICE_RESULT_STATUS 

    FROM filtered_imaging_studies

    INNER JOIN 
      `ml-mps-adl-intfhr-phi-p-3b6e.phi_secondary_use_fhir_clinicnumber_us_p.Endpoint` ENDPOINT 
      ON (filtered_imaging_studies.gcp_endpoint_id = ENDPOINT.id) 

    INNER JOIN 
      `ml-mps-adl-intfhr-phi-p-3b6e.phi_secondary_use_fhir_clinicnumber_us_p.Patient` PAT_PATIENT 
      ON (filtered_imaging_studies.clinic_number = PAT_PATIENT.clinic_number) 

    LEFT JOIN 
      `ml-mps-adl-intudp-phi-p-d5cb.phi_udpwh_etl_us_p.FACT_RADIOLOGY` RAD_FACT_RADIOLOGY 
      ON (filtered_imaging_studies.ACCESSION_IDENTIFIER_VALUE = RAD_FACT_RADIOLOGY.ACCESSION_NBR)
    """
    
    # Add limit clause if specified
    if limit is not None:
        query += f"\nLIMIT {limit}"
        print(f"Running query with limit of {limit} rows...")
    else:
        print("Running query without row limit...")
    
    # Execute query and return results as dataframe
    query_start_time = time.time()
    print("Executing query...")
    df = client.query(query).to_dataframe()
    query_end_time = time.time()
    query_duration = query_end_time - query_start_time
    
    print(f"Query complete. Retrieved {len(df)} rows in {query_duration:.2f} seconds.")
    
    return df

# With limit
print("Starting breast imaging query process...")
start_time = time.time()
df_limited = run_breast_imaging_query(limit=10)

# Save to CSV
output_file = 'dicom_urls.csv'
print(f"\nSaving results to {output_file}...")
df_limited.to_csv(output_file, index=False)
end_time = time.time()
total_duration = end_time - start_time

print(f"Results successfully saved to {output_file}")