import os
import subprocess
import boto3
from datetime import datetime
import logging
import sys
from botocore.exceptions import ClientError

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Configuration
DB_HOST = '10.247.0.4'
DB_PORT = '5432'
DB_USER = 'catalog'
DB_NAME = 'catalog'
DB_SCHEMA = '1763363716'
DB_PASSWORD = '3ayhojxCDBkD5kBkqY0Xl9nBCqQgX51O'

PG_DUMP_PATH = '/usr/pgsql-15/bin/pg_dump'

S3_ENDPOINT = 'https://axownvq9lhmx.compat.objectstorage.me-jeddah-1.oraclecloud.com'
S3_ACCESS_KEY = '721874726f3bf72a9b5db70d12a5ffa3fec4e3bc'
S3_SECRET_KEY = 'aHEScfZ+b9R68vfIejs0YkP2BWuzCuzpGIdKNgqKfJw='
S3_REGION = 'me-jeddah-1'
S3_BUCKET = 'byte-dance-catalog-pgdump'

def create_schema_dump():
    """Create PostgreSQL dump for specific schema with version handling"""
    try:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{DB_SCHEMA}_backup_{timestamp}.dump"
        filepath = f"{filename}"

        logger.info(f"Starting backup for schema: {DB_SCHEMA}")

        os.environ['PGPASSWORD'] = DB_PASSWORD

        # Build pg_dump
        cmd = [
            PG_DUMP_PATH,
            '-h', DB_HOST,
            '-p', DB_PORT,
            '-U', DB_USER,
            '-d', DB_NAME,
            '-n', DB_SCHEMA,
            '-F', 'c',
            '-f', filepath,
            '-v',
            '--no-sync'
        ]

        result = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )

        if result.returncode != 0:
            logger.error(f"pg_dump failed with exit code {result.returncode}")
            logger.error(f"Error details: {result.stderr}")
            return None, None

        if not os.path.exists(filepath):
            logger.error("Backup file creation failed: file not found")
            return None, None

        if os.path.getsize(filepath) == 0:
            logger.error("Backup file creation failed: empty file")
            return None, None

        logger.info(f"Backup created successfully: {filepath} ({os.path.getsize(filepath)} bytes)")
        return filepath, filename

    except Exception as e:
        logger.exception(f"Critical error during backup: {str(e)}")
        return None, None

def upload_to_s3(filepath, filename):
    """Upload file to OCI-compatible S3 storage"""
    if not filepath or not filename:
        logger.error("Invalid parameters for S3 upload")
        return False

    try:
        logger.info(f"Initializing S3 connection to {S3_ENDPOINT}")

        # Create S3 client
        s3 = boto3.client(
            's3',
            endpoint_url=S3_ENDPOINT,
            aws_access_key_id=S3_ACCESS_KEY,
            aws_secret_access_key=S3_SECRET_KEY,
            region_name=S3_REGION
        )

        # Upload file
        logger.info(f"Uploading {filename} to bucket: {S3_BUCKET}")
        s3.upload_file(filepath, S3_BUCKET, filename)

        logger.info("Upload completed successfully")
        return True

    except ClientError as e:
        logger.error(f"S3 client error: {e.response['Error']['Message']}")
        return False
    except Exception as e:
        logger.exception(f"Error during S3 upload: {str(e)}")
        return False

def cleanup(filepath):
    """Remove local backup file"""
    if not filepath:
        return

    try:
        if os.path.exists(filepath):
            os.remove(filepath)
            logger.info(f"Cleaned up local file: {filepath}")
    except Exception as e:
        logger.error(f"Error during cleanup: {str(e)}")

def main():
    """Main backup workflow"""
    logger.info("Starting database backup process")

    # Create database dump
    filepath, filename = create_schema_dump()
    if not filepath:
        logger.error("Backup creation failed. Exiting.")
        cleanup(filepath)
        sys.exit(1)

    # Upload to S3
    if not upload_to_s3(filepath, filename):
        logger.error("S3 upload failed. Local backup retained.")
        cleanup(filepath)
        sys.exit(1)

    # Cleanup local file
    cleanup(filepath)

    logger.info("Backup completed successfully")

if __name__ == "__main__":
    main()

