import os
import logging
import boto3
from botocore.exceptions import ClientError
from botocore.config import Config
from tqdm import tqdm
from dotenv import load_dotenv # Import the library to load .env file

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - [%(name)s] - %(message)s'
    )
    return logging.getLogger('oci-uploader')

def validate_directory(path):
    if not os.path.isdir(path):
        raise FileNotFoundError(f"Directory not found: {path}")
    if not os.access(path, os.R_OK):
        raise PermissionError(f"Cannot read directory: {path}")

def create_oci_client(config_values):
    # This function creates the S3 client.
    # 'payload_signing_enabled': True is kept to help with Content-Length issues.
    return boto3.client(
        's3',
        endpoint_url=config_values['endpoint'],
        aws_access_key_id=config_values['key_id'],
        aws_secret_access_key=config_values['secret_key'],
        region_name=config_values['region'],
        config=Config(
            signature_version='s3v4',
            s3={
                'addressing_style': 'path',
                'payload_signing_enabled': True
            },
            retries={'max_attempts': 3},
            connect_timeout=30,
            read_timeout=60
        )
    )

def verify_bucket_access(s3_client, bucket_name):
    logger = logging.getLogger('oci-uploader.verify_bucket')
    try:
        logger.debug(f"Verifying bucket '{bucket_name}'.")
        s3_client.head_bucket(Bucket=bucket_name)
        logger.info(f"Access to bucket '{bucket_name}' verified.")
    except ClientError as e:
        error_code = e.response.get('Error', {}).get('Code', 'ERROR')
        error_message = e.response.get('Error', {}).get('Message', str(e))
        logger.error(f"Error accessing bucket '{bucket_name}': {error_code} - {error_message}")
        if error_code == '404':
            raise ValueError(f"Bucket does not exist: {bucket_name}")
        if error_code == '403':
            raise PermissionError(f"Access denied to bucket: {bucket_name}")
        raise

def collect_valid_files(local_dir_path):
    logger = logging.getLogger('oci-uploader.collect_files')
    valid_files_list = []
    for root_dir, _, file_names in os.walk(local_dir_path):
        for file_name in file_names:
            full_path = os.path.join(root_dir, file_name)
            try:
                if not os.path.isfile(full_path) or not os.access(full_path, os.R_OK):
                    continue
                if os.path.getsize(full_path) == 0:
                    logger.info(f"Skipping empty file: {full_path}")
                    continue
                relative_path = os.path.relpath(full_path, local_dir_path)
                object_key_name = relative_path.replace(os.sep, '/')
                valid_files_list.append((full_path, object_key_name))
            except Exception as e:
                logger.error(f"Error collecting file {full_path}: {e}")
    return valid_files_list

def upload_to_oci(s3_client, bucket_name, files_to_upload_list):
    logger = logging.getLogger('oci-uploader.upload')
    successful_uploads = []
    failed_uploads = []

    for local_file_path, object_key in tqdm(files_to_upload_list, desc="Uploading"):
        try:
            s3_client.upload_file(
                Filename=local_file_path,
                Bucket=bucket_name,
                Key=object_key,
                ExtraArgs={'ContentType': 'application/octet-stream'}
            )
            successful_uploads.append(object_key)
        except Exception as e:
            error_code = 'UNKNOWN'
            error_message = str(e)
            if isinstance(e, ClientError):
                error_code = e.response.get('Error', {}).get('Code', 'ERROR')
                error_message = e.response.get('Error', {}).get('Message', str(e))
            if "Failed to upload" in str(e) and "MissingContentLength" in str(e):
                 error_message = str(e)

            failed_uploads.append({'key': object_key, 'error': error_message, 'code': error_code})
            logger.error(f"Upload failed for {object_key}: Code={error_code}, Message={error_message}")

    return successful_uploads, failed_uploads

def generate_upload_report(successful_uploads_list, failed_uploads_list):
    print(f"\n{'='*50}\nUpload Report:")
    print(f"✅ Successful uploads: {len(successful_uploads_list)}")
    print(f"❌ Failed uploads: {len(failed_uploads_list)}")
    if failed_uploads_list:
        print("\nFailure Details:")
        for failure_info in failed_uploads_list:
            print(f"- File: {failure_info['key']}\n  Error Code: {failure_info['code']}\n  Error Message: {failure_info['error']}\n")

def main():
    app_logger = setup_logging()
    
    # Load environment variables from .env file
    load_dotenv()

    # Fetch configuration from environment variables
    # Default values can be provided if an environment variable might be optional
    CONFIG = {
        'key_id': os.environ.get('OCI_ACCESS_KEY_ID'),
        'secret_key': os.environ.get('OCI_SECRET_ACCESS_KEY'),
        'endpoint': os.environ.get('OCI_ENDPOINT_URL'),
        'bucket': os.environ.get('OCI_BUCKET_NAME', 'random-logs'), # Default if not in .env
        'local_dir': os.environ.get('OCI_LOCAL_DIR', 'random-logs'), # Default if not in .env
        'region': os.environ.get('OCI_REGION', 'me-jeddah-1')     # Default if not in .env
    }

    # Validate that essential variables are loaded
    missing_vars = [var for var in ['key_id', 'secret_key', 'endpoint'] if not CONFIG[var]]
    if missing_vars:
        app_logger.critical(f"CRITICAL: Missing required environment variables in .env file or environment: {', '.join(var.upper() for var in missing_vars)}")
        app_logger.critical("Please ensure OCI_ACCESS_KEY_ID, OCI_SECRET_ACCESS_KEY, and OCI_ENDPOINT_URL are set.")
        exit(4)

    app_logger.info("Script starting...")
    config_to_log = {k: (v if k != 'secret_key' else '********') for k, v in CONFIG.items()} # Hide secret key in logs
    app_logger.debug(f"Current configuration from environment: {config_to_log}")

    try:
        validate_directory(CONFIG['local_dir'])
        s3_client = create_oci_client(CONFIG)
        verify_bucket_access(s3_client, CONFIG['bucket'])

        app_logger.info(f"Collecting files from: '{CONFIG['local_dir']}'")
        files_to_upload = collect_valid_files(CONFIG['local_dir'])
        
        if not files_to_upload:
            app_logger.warning(f"No valid files found to upload in '{CONFIG['local_dir']}'.")
            generate_upload_report([], [])
            return

        app_logger.info(f"Starting upload of {len(files_to_upload)} files to bucket '{CONFIG['bucket']}'...")
        successful_uploads, failed_uploads = upload_to_oci(s3_client, CONFIG['bucket'], files_to_upload)
        generate_upload_report(successful_uploads, failed_uploads)
        
        exit(1 if failed_uploads else 0)

    except ClientError as e:
        error_code = e.response.get('Error', {}).get('Code', 'API_ERROR')
        error_message = e.response.get('Error', {}).get('Message', str(e))
        app_logger.error(f"OCI API Error ({error_code}): {error_message}")
        app_logger.debug("Full ClientError details:", exc_info=True)
        exit(2)
    except (FileNotFoundError, PermissionError, ValueError) as e:
        app_logger.error(f"Setup or Configuration error: {str(e)}")
        app_logger.debug("Full Setup/Configuration error details:", exc_info=True)
        exit(3)
    except Exception as e:
        app_logger.error(f"An unexpected critical error occurred: {str(e)}")
        app_logger.debug("Full critical error details:", exc_info=True)
        exit(3)

if __name__ == "__main__":
    main()
