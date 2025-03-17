#!/usr/bin/env python3

import csv
import os
import subprocess
import tqdm
from google.cloud import pubsub_v1



# Environment configuration
PROJECT_ID = "aif-usr-p-rad-brstai-87d1"
REGION = "us-central1"
TOPIC_NAME = "dicom-processing-topic"
SUBSCRIPTION_NAME = "dicom-processing-subscription"
MY_SERVICE_ACCOUNT = "M302453"
SERVICE_ACCOUNT_IDENTITY = f"gsa6-va-prj-aif-p-{MY_SERVICE_ACCOUNT}-87d1@{PROJECT_ID}.iam.gserviceaccount.com"

# Cloud Run configuration
CLOUD_RUN_SERVICE = "pubsub-push-cloudrun"
VERSION = "1.0"
AR = f"shared-aif-artifact-registry-docker-87d1"
AR_NAME = "pubsub_push_cloudrun"
TARGET_TAG = f"us-central1-docker.pkg.dev/{PROJECT_ID}/{AR}/{AR_NAME}:{VERSION}"
VPC_SHARED = "aif-env-sharedvpc-148f"
VPC_NAME = "aif-vpc-p-ops-auto-01"

# Cloud Build configuration
CONTENT_DIR = os.path.dirname(os.path.abspath(__file__))  # Directory of this script
FASTAPI_DIR = os.path.join(CONTENT_DIR, "_fastapi")  # Assuming _fastapi directory exists
GCS_LOG = f"gs://shared-aif-bucket-87d1/cloudbuild_log"
GCS_STAGE = f"gs://shared-aif-bucket-87d1/cloudbuild_stage"

# The URL will be obtained after deployment
CLOUD_RUN_URL = None





def build_and_push_image():
    """Builds and pushes the Docker image using Cloud Build."""
    print("Building and pushing Docker image...")
    
    if not os.path.exists(FASTAPI_DIR):
        print(f"ERROR: FastAPI directory not found: {FASTAPI_DIR}")
        raise FileNotFoundError(f"Directory not found: {FASTAPI_DIR}")
    
    command = [
        "gcloud", "builds", "submit",
        "--gcs-source-staging-dir", GCS_STAGE,
        "--gcs-log-dir", GCS_LOG,
        "--tag", TARGET_TAG,
        ".",
    ]
    
    try:
        print(f"Executing in directory: {FASTAPI_DIR}")
        result = subprocess.run(command, cwd=FASTAPI_DIR, check=True, capture_output=True, text=True)
        print(f"Build successful: {result.stdout}")
        return True
    except subprocess.CalledProcessError as e:
        print(f"Build failed: {e.stderr}")
        raise


def deploy_cloud_run():
    """Deploy the FastAPI application to Cloud Run."""
    global CLOUD_RUN_URL
    
    print("Deploying Cloud Run service...")
    
    cr_name = AR_NAME.replace("_", "-")
    vpc_connector = f"projects/{VPC_SHARED}/locations/{REGION}/connectors/{VPC_NAME}"
    
    command = [
        "gcloud", "run", "deploy", cr_name,
        "--binary-authorization=default",
        f"--image={TARGET_TAG}",
        "--ingress=internal-and-cloud-load-balancing",
        "--no-allow-unauthenticated",
        "--port=5000",
        f"--project={PROJECT_ID}",
        "--quiet",
        f"--region={REGION}",
        f"--service-account={SERVICE_ACCOUNT_IDENTITY}",
        f"--vpc-connector={vpc_connector}",
        "--vpc-egress=all-traffic",
        "--timeout=3000",
        "--memory=1024Mi",
    ]
    
    try:
        result = subprocess.run(command, check=True, capture_output=True, text=True)
        print(f"Successfully deployed Cloud Run service: {cr_name}")
        
        # Get the URL of the deployed service
        url_command = ["gcloud", "run", "services", "describe", cr_name, 
                       f"--region={REGION}", f"--project={PROJECT_ID}", 
                       "--format=value(status.url)"]
        url_result = subprocess.run(url_command, check=True, capture_output=True, text=True)
        CLOUD_RUN_URL = url_result.stdout.strip()
        print(f"Cloud Run URL: {CLOUD_RUN_URL}")
        
        return CLOUD_RUN_URL
    except subprocess.CalledProcessError as e:
        print(f"Error deploying Cloud Run service: {e.stderr}")
        raise


def setup_pubsub():
    """Create Pub/Sub topic and subscription with push configuration."""
    global CLOUD_RUN_URL
    
    if CLOUD_RUN_URL is None:
        print("ERROR: Cloud Run URL is not available. Deploy Cloud Run first.")
        return False
    
    # The endpoint where Pub/Sub will push messages
    push_endpoint = f"{CLOUD_RUN_URL}/push_handlers/receive_messages"
    print(f"Push endpoint: {push_endpoint}")
    
    print(f"Setting up Pub/Sub topic: {TOPIC_NAME}")
    try:
        # Create topic
        subprocess.run([
            "gcloud", "pubsub", "topics", "create", TOPIC_NAME,
            f"--project={PROJECT_ID}"
        ], check=True)
        print(f"Topic '{TOPIC_NAME}' created successfully.")
    except subprocess.CalledProcessError as e:
        if "already exists" in str(e):
            print(f"Topic '{TOPIC_NAME}' already exists.")
        else:
            raise
    
    print(f"Setting up Pub/Sub subscription: {SUBSCRIPTION_NAME}")
    try:
        # Create subscription with push configuration
        subprocess.run([
            "gcloud", "pubsub", "subscriptions", "create", SUBSCRIPTION_NAME,
            f"--topic={TOPIC_NAME}",
            f"--push-endpoint={push_endpoint}",
            f"--push-auth-service-account={SERVICE_ACCOUNT_IDENTITY}",
            f"--project={PROJECT_ID}"
        ], check=True)
        print(f"Subscription '{SUBSCRIPTION_NAME}' created successfully.")
    except subprocess.CalledProcessError as e:
        if "already exists" in str(e):
            print(f"Subscription '{SUBSCRIPTION_NAME}' already exists.")
        else:
            raise
    
    return True


def publish_message(url):
    global PUBLISHER
    global TOPIC_PATH
    
    future = PUBLISHER.publish(TOPIC_PATH, data=url.encode("utf-8"))
    return future

def process_csv_file(csv_file):
    """
    Read the CSV file containing DICOM URLs and publish each URL to Pub/Sub.
    
    Args:
        csv_file (str): Path to the CSV file
    """
    print(f"Processing CSV file: {csv_file}")
    
    # First count total rows for the progress bar
    with open(csv_file, 'r') as f:
        total_rows = sum(1 for _ in csv.DictReader(f))
    
    num_processed = 0
    with open(csv_file, 'r') as f:
        reader = csv.DictReader(f)
        # Create progress bar
        pbar = tqdm.tqdm(total=total_rows, desc="Publishing messages")
        for row in reader:
            url = row.get('ENDPT_ADDRESS')
            if not url:
                print(f"Warning: Missing URL in row: {row}")
                pbar.update(1)
                continue
                
            publish_message(url)
            num_processed += 1
            pbar.update(1)
            
        pbar.close()
    
    print(f"Processed {num_processed} URLs from {csv_file}")


def cleanup_resources(delete_cloud_run=False):
    """
    Delete created resources.
    
    Args:
        delete_cloud_run (bool): Whether to delete the Cloud Run service
    """
    print("Cleaning up resources...")
    
    # Delete subscription
    try:
        subprocess.run([
            "gcloud", "pubsub", "subscriptions", "delete", SUBSCRIPTION_NAME,
            f"--project={PROJECT_ID}", "--quiet"
        ], check=True)
        print(f"Subscription '{SUBSCRIPTION_NAME}' deleted.")
    except subprocess.CalledProcessError as e:
        print(f"Failed to delete subscription '{SUBSCRIPTION_NAME}': {e}")
    
    # Delete topic
    try:
        subprocess.run([
            "gcloud", "pubsub", "topics", "delete", TOPIC_NAME,
            f"--project={PROJECT_ID}", "--quiet"
        ], check=True)
        print(f"Topic '{TOPIC_NAME}' deleted.")
    except subprocess.CalledProcessError as e:
        print(f"Failed to delete topic '{TOPIC_NAME}': {e}")
    
    # Delete Cloud Run service if requested
    if delete_cloud_run:
        try:
            cr_name = AR_NAME.replace("_", "-")
            subprocess.run([
                "gcloud", "run", "services", "delete", cr_name,
                f"--region={REGION}", f"--project={PROJECT_ID}", "--quiet"
            ], check=True)
            print(f"Cloud Run service '{cr_name}' deleted.")
        except subprocess.CalledProcessError as e:
            print(f"Failed to delete Cloud Run service: {e}")
        
        # Also delete the container image
        try:
            registry = f"us-central1-docker.pkg.dev/{PROJECT_ID}/{AR}/{AR_NAME}"
            subprocess.run([
                "gcloud", "artifacts", "docker", "images", "delete", 
                registry, "--quiet", "--delete-tags"
            ], check=True)
            print(f"Container image '{registry}' deleted.")
        except subprocess.CalledProcessError as e:
            print(f"Failed to delete container image: {e}")





def dicom_download_remote_start(csv_file = None, deploy = False, cleanup = False):
    global CLOUD_RUN_URL
    global PUBLISHER
    global TOPIC_PATH
    
    
    PUBLISHER = pubsub_v1.PublisherClient()
    TOPIC_PATH = PUBLISHER.topic_path(PROJECT_ID, TOPIC_NAME)
    
    # Handle cleanup first - this can be run without other flags
    if cleanup:
        cleanup_resources(delete_cloud_run=True)
        return 0
    
    # For other operations, we need a CSV file
    if not csv_file and (deploy):
        print("Error: CSV file is required for deployment or setup operations")
        return 1
    
    if deploy:
        # Build and push the image first, then deploy to Cloud Run
        build_and_push_image()
        deploy_cloud_run()
    elif CLOUD_RUN_URL is None:
        # If Cloud Run URL isn't set and we're not deploying, use the hardcoded URL
        CLOUD_RUN_URL = f"https://{CLOUD_RUN_SERVICE}-243026470979.{REGION}.run.app"
        print(f"Using existing Cloud Run URL: {CLOUD_RUN_URL}")
    
    if deploy:
        setup_pubsub()
    
    if csv_file:
        if os.path.exists(csv_file):
            process_csv_file(csv_file)
        else:
            print(f"Error: CSV file not found: {csv_file}")
            return 1
    
    return 0