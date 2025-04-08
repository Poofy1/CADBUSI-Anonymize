# CADBUSI-Anonymize

This repository anonymizes and retrieves ultrasound image data from the Mayo Clinic database. 

## Setup

- Create a Mayo Clinic AI Factory instance
- Clone repository: `git clone https://github.com/Poofy1/CADBUSI-Anonymize.git`
- Install requirements: `pip install -r requirements.txt`
- Configure: `config.py`
- Obtain certificate `./src/_fastapi/CertEmulationCA.crt`

## Usage

The pipeline is operated through a single command-line interface in main.py, which provides several functions:

### Querying Data

To query breast imaging data:
`python main.py --query [optional: limit=N]`

This will:
1. Run a query to retrieve breast imaging records
2. Filter and clean the radiology and pathology data
3. Create a final dataset for processing
4. Save results to `output/endpoint_data.csv`

Example with a limit:`python main.py --query limit=100`


### Downloading DICOM Files

The tool offers Cloud Run deployment for efficient DICOM downloads. Dicoms will appear in specified GCP bucket storage:
```
# Deploy the FastAPI service to Cloud Run and start dicom data download (REQUIRED)
python main.py --deploy

# Resend the download requests to the pre-deployed service (OPTIONAL)
python main.py --rerun 

# Clean up Cloud Run resources when finished (REQUIRED)
python main.py --cleanup
```
### Anonymizing DICOM Files

To anonymize downloaded DICOM files:

`python main.py --anon [source-bucket-location]`

This will:
1. Generate encryption keys for safely anonymizing patient IDs
2. Deidentify DICOM files from the source bucket
3. Store anonymized files in the specified output directory in the destination bucket

Example:

`python main.py --anon "2025-04-01_221610"`

## Query Diagram `--query [optional: limit=N]`
![CASBUSI Query](/demo/CADBUSI_Query.png)

## Data Pipeline
- [CADBUSI-Anonymize](https://github.com/Poofy1/CADBUSI-Anonymize)
- [CADBUSI-Database](https://github.com/Poofy1/CADBUSI-Database)
- [CADBUSI-Training](https://github.com/Poofy1/CADBUSI-Training)
![CADBUSI Pipeline](https://raw.githubusercontent.com/Poofy1/CADBUSI-Database/main/pipeline/CADBUSI-Pipeline.png)

