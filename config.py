CONFIG = {
    # Environment configuration
    "env": {
        "project_id": "your-project-id",
        "region": "your-region",
        "topic_name": "your-pubsub-topic",
        "subscription_name": "your-pubsub-subscription",
        "my_service_account": "your-service-account-id",
        "service_account_identity": "your-service-account@your-project-id.iam.gserviceaccount.com"
    },
    
    # Cloud Run configuration
    "cloud_run": {
        "service": "your-cloud-run-service-name",
        "version": "1.0",
        "ar": "your-artifact-registry",
        "ar_name": "your-artifact-name",
        "target_tag": "region-docker.pkg.dev/your-project-id/your-artifact-registry/your-artifact-name:version",
        "vpc_shared": "your-shared-vpc-name",
        "vpc_name": "your-vpc-name"
    },
    
    # Storage configuration
    "storage": {
        "gcs_log": "gs://your-bucket-name/cloudbuild_log",
        "gcs_stage": "gs://your-bucket-name/cloudbuild_stage",
        "bucket_name": "your-bucket-name",
        "download_path": "Downloads",
        "anonymized_path": "anon_dicoms"
    }
}