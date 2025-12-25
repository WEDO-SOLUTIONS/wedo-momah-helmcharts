#!/usr/bin/env python3
import oci
import os

# Configuration - use different variable name to avoid conflict
app_config = {
    "namespace": "axownvq9lhmx",
    "bucket_name": "adam-logs",
    "file_path": "KXB9.txt",
    "object_name": "KXB9.txt",
    "endpoint": "https://axownvq9lhmx.objectstorage.me-jeddah-1.oci.customer-oci.com"
}

def upload_to_oci():
    try:
        # Initialize OCI client - uses default config from ~/.oci/config
        oci_config = oci.config.from_file()
        object_storage = oci.object_storage.ObjectStorageClient(config=oci_config)
        object_storage.base_client.endpoint = app_config['endpoint']
        
        # Get namespace
        namespace = object_storage.get_namespace().data
        
        # Upload file
        print(f"Uploading {app_config['file_path']} to bucket {app_config['bucket_name']}...")
        
        with open(app_config['file_path'], 'rb') as file:
            object_storage.put_object(
                namespace_name=namespace,
                bucket_name=app_config['bucket_name'],
                object_name=app_config['object_name'],
                put_object_body=file
            )
        
        print("✅ Upload successful!")
        
        # Make object public by updating metadata
        print("Making object publicly accessible...")
        object_storage.update_object(
            namespace_name=namespace,
            bucket_name=app_config['bucket_name'],
            object_name=app_config['object_name'],
            update_object_details=oci.object_storage.models.UpdateObjectDetails(
                metadata={'opc-meta-anyone-can-read': 'true'}
            )
        )
        
        print("✅ Object is now public!")
        print(f"Public URL: {app_config['endpoint']}/n/{namespace}/b/{app_config['bucket_name']}/o/{app_config['object_name']}")
        
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    upload_to_oci()
