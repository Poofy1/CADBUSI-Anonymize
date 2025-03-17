import logging
import re
import io
import sys
import base64
import json
from typing import Union
from typing import Dict
from fastapi.logger import logger
from fastapi import FastAPI
from starlette.status import HTTP_204_NO_CONTENT
from fastapi.responses import JSONResponse
from starlette.requests import Request
from starlette.responses import Response
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.middleware.base import RequestResponseEndpoint
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.middleware.base import RequestResponseEndpoint
from google.cloud.logging.handlers import CloudLoggingFilter

# Dicom imports
import requests
import pydicom as dicom
from requests.structures import CaseInsensitiveDict
from requests_toolbelt import MultipartDecoder
from google.cloud import storage
import google.auth.transport.requests


# Initialize Google Cloud Logging client
import google.cloud.logging
import google.oauth2.id_token
import google.auth.transport.requests


# Create a custom logging filter
class GoogleCloudLogFilter(CloudLoggingFilter):
    def filter(self, record: logging.LogRecord) -> bool:
        """Add HTTP request and Cloud Trace context to log records."""
        record.http_request = self._get_http_request_info(record)
        record.trace, record.span_id = self._get_trace_info(record)
        return super().filter(record)

    def _get_http_request_info(self, record) -> Dict:
        """Extract HTTP request information from the record."""
        request = record.__dict__.get("request", None)
        if not request:
            return {}

        return {
            "requestMethod": request.method,
            "requestUrl": request.url.path,
            "requestSize": sys.getsizeof(request),
            "remoteIp": request.client.host,
            "protocol": request.url.scheme,
            "referrer": request.headers.get("referrer"),
            "userAgent": request.headers.get("user-agent"),
        }

    def _get_trace_info(self, record) -> tuple[str, str]:
        """Extract Cloud Trace context from the record."""
        trace = record.__dict__.get("cloud_trace_context", "")
        split_header = trace.split("/", 1)
        trace = (
            f"projects/{self.project}/traces/{split_header[0]}" if split_header else ""
        )
        span_id = (
            re.findall(r"^\w+", split_header[1])[0] if len(split_header) > 1 else ""
        )
        return trace, span_id


# --- Middleware ---
class LoggingMiddleware(BaseHTTPMiddleware):
    async def dispatch(
        self, request: Request, call_next: RequestResponseEndpoint
    ) -> Response:
        # Store Cloud Trace context in the request state
        request.state.cloud_trace_context = request.headers.get(
            "x-cloud-trace-context", ""
        )
        try:
            response = await call_next(request)
            return response
        except Exception as ex:
            logger.exception(f"Request failed: {ex}")
            return JSONResponse(
                status_code=500, content={"success": False, "message": str(ex)}
            )


app = FastAPI()
# Initialize Google Cloud Logging client
client = google.cloud.logging.Client()
handler = client.get_default_handler()

# set the level to show above
handler.setLevel(logging.DEBUG)

# Add the custom filter to the handler
handler.addFilter(GoogleCloudLogFilter(project=client.project))

# Configure the logger to use the handler
logging.getLogger().handlers = []
logging.getLogger().addHandler(handler)
logger.setLevel(logging.DEBUG)


@app.get("/")
async def read_root():
    logger.info("INFO MSG")
    logger.debug("DEBUG MSG")
    logger.warning("WARNING MSG")
    logger.error("ERROR MSG")
    return {"Hello": "World"}


@app.get("/items/{item_id}")
async def read_item(item_id: int, q: Union[str, None] = None):
    return {"item_id": item_id, "q": q}


# Helper function to verify JWT
def verify_jwt(token: str) -> Dict:
    """Verifies a JWT token and returns the claims."""
    auth_req = google.auth.transport.requests.Request()
    return google.oauth2.id_token.verify_oauth2_token(token, auth_req)


def get_oauth2_token():
    """Retrieves an OAuth2 token for accessing the Google Cloud Healthcare API."""
    creds, project = google.auth.default()
    auth_req = google.auth.transport.requests.Request()
    creds.refresh(auth_req)
    return creds.token
# Add a function to retrieve and store DICOM images
async def retrieve_and_store_dicom(url, bucket_name, bucket_path):
    """
    Retrieves a DICOM image from the given URL and stores it in Google Cloud Storage.
    
    Args:
        url (str): URL to retrieve the DICOM image from
        bucket_name (str): GCS bucket name
        bucket_path (str): Path within the bucket
    
    Returns:
        bool: Success status
    """
    # Set up headers for DICOM request
    headers = CaseInsensitiveDict()
    headers['Accept'] = 'multipart/related; type="application/dicom"; transfer-syntax=*'
    headers["Authorization"] = f"Bearer {get_oauth2_token()}"
    
    try:
        # Get the DICOM image
        response = requests.get(url, headers=headers)
        
        # Check response status
        if response.status_code != 200:
            logger.error(f"Failed to retrieve DICOM from {url}: Status {response.status_code}")
            return False
            
        # Get content type to determine how to handle the response
        content_type = response.headers.get('Content-Type', '')
        
        if 'application/json' in content_type:
            # Handle JSON response (likely an error or metadata)
            logger.warning(f"Received JSON response instead of DICOM: {response.json()}")
            return False
            
        elif 'multipart/related' in content_type:
            # Handle multipart DICOM response
            decoder = MultipartDecoder(response.content, content_type)
            
            # Process each part of the multipart response
            for part in decoder.parts:
                content = part.content
                byte_stream = io.BytesIO(content)
                
                try:
                    # Read DICOM data
                    dcm = dicom.dcmread(byte_stream, force=True)
                    
                    # Get instance UID for filename
                    instance_uid = str(dcm['SOPInstanceUID'].value)
                    
                    # Create path and upload to GCS
                    file_path = f"{bucket_path}/image_instance/{instance_uid}.dcm"
                    
                    # Upload to Google Cloud Storage
                    storage_client = storage.Client()
                    bucket = storage_client.bucket(bucket_name)
                    blob = bucket.blob(file_path)
                    blob.upload_from_string(content)
                    
                    logger.info(f"Successfully uploaded DICOM to {bucket_name}/{file_path}")
                    return True
                    
                except Exception as e:
                    logger.exception(f"Error processing DICOM part: {e}")
            
            logger.warning(f"No valid DICOM parts found in response from {url}")
            return False
            
        else:
            logger.error(f"Unexpected content type: {content_type} from {url}")
            return False
            
    except Exception as e:
        logger.exception(f"Error retrieving DICOM from {url}: {e}")
        return False

# Modify the Pub/Sub handler
@app.post("/push_handlers/receive_messages")
async def pubsub_push_handlers_receive(request: Request):
    bearer_token = request.headers.get("Authorization")
    if not bearer_token:
        return JSONResponse(
            status_code=401, content={"message": "Missing Authorization header"}
        )

    try:
        token = bearer_token.split(" ")[1]
        claim = verify_jwt(token)
        logger.info("JWT Claim:", extra={"claim": claim})

        envelope = await request.json()
        if (
            isinstance(envelope, dict)
            and "message" in envelope
            and "data" in envelope["message"]
        ):
            # Decode the Pub/Sub message data
            data = base64.b64decode(envelope["message"]["data"]).decode("utf-8")
            dicom_url = data  # Now data is directly the URL instead of a JSON payload

            if not dicom_url:
                logger.error("No DICOM URL found in payload")
                return JSONResponse(
                    status_code=400, content={"message": "No DICOM URL in payload"}
                )
            
            # Set your bucket information
            bucket_name = "shared-aif-bucket-87d1"  # Replace with your actual bucket name
            bucket_path = "Downloads"               # Replace with your actual path
            
            # Retrieve and store the DICOM image
            success = await retrieve_and_store_dicom(dicom_url, bucket_name, bucket_path)
            
            if success:
                logger.info(f"Successfully processed DICOM from {dicom_url}")
            else:
                logger.warning(f"Failed to process DICOM from {dicom_url}")
            
        else:
            logger.warning(
                "Invalid Pub/Sub message format", extra={"envelope": envelope}
            )

        # Return a 204 to indicate a success, even if processing failed
        # This is standard practice for Pub/Sub to prevent retries
        return Response(status_code=HTTP_204_NO_CONTENT)
    except (ValueError, IndexError) as e:
        logger.error(f"Invalid Authorization header format: {e}")
        return JSONResponse(
            status_code=401, content={"message": "Invalid Authorization header"}
        )
    except Exception as e:
        logger.exception(f"Error processing Pub/Sub message: {e}")
        return JSONResponse(
            status_code=500, content={"message": "Error processing message"}
        )
    
    

app.add_middleware(LoggingMiddleware)  # Register the middleware

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=5000)