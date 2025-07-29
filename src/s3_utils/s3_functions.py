import os
import boto3
from typing import Dict, List, Any, Optional
from dotenv import load_dotenv
from botocore.exceptions import ClientError, NoCredentialsError
from flask import Flask, send_file
from io import BytesIO
import sys
import logging
import base64

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)


# Common utility functions
def get_full_path(path: str) -> str:
    """Convert a path to an absolute path."""
    return os.path.abspath(os.path.expanduser(path))

def format_error_response(error: str, details: Optional[str] = None) -> Dict[str, Any]:
    """Format a standardized error response."""
    response = {"error": error}
    if details:
        response["details"] = details
    return response

class S3Client:
    """
    AWS S3 Client wrapper for basic operations.
    """
    def __init__(self, access_key: str, secret_key: str, region_name: str = "eu-central-1"):
        self.s3 = boto3.client(
            "s3",
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region_name
        )

    def list_buckets(self):
        """
        List all S3 buckets.
        
        Returns:
            list: List of bucket names, or empty list on error
        """
        try:
            response = self.s3.list_buckets()
            buckets = [bucket['Name'] for bucket in response.get('Buckets', [])]
            print(f"Found buckets: {buckets}")
            return {"status": "success", "bucket": buckets, "buckets": buckets}
        except ClientError as e:
            print(f"Error listing buckets: {str(e)}")
            return format_error_response("Failed to list", str(e))

    def upload_file(self, file_path: str, bucket: str, object_name: Optional[str] = None) -> Dict[str, Any]:
        if object_name is None:
            object_name = os.path.basename(file_path)
        try:
            self.s3.upload_file(file_path, bucket, object_name)
            return {"status": "success", "bucket": bucket, "object_name": object_name}
        except Exception as e:
            return format_error_response("Failed to upload file to S3", str(e))

    def upload_to_s3(self, base64_string, bucket_name, s3_key, aws_region='us-east-1'):
        """
        Upload base64-encoded data to an S3 bucket after decoding it.
        
        Args:
            base64_string (str): Base64-encoded data
            bucket_name (str): Name of the S3 bucket
            s3_key (str): S3 object key (path/filename in bucket)
            aws_region (str): AWS region of the S3 bucket
        """
        try:      
            # Decode base64 string to binary
            decoded_data = base64.b64decode(base64_string)
            
            # Upload to S3
            self.s3.put_object(
                Bucket=bucket_name,
                Key=s3_key,
                Body=decoded_data
            )
            print(f"Successfully uploaded data to s3://{bucket_name}/{s3_key}")
            
        except ClientError as e:
            print(f"Error uploading to S3: {e}")
            sys.exit(1)
        except base64.binascii.Error:
            print("Error: Invalid base64 string")
            sys.exit(1)
        except Exception as e:
            print(f"Unexpected error: {e}")
            sys.exit(1)

    def s3_uploadfile_from_bytes(self, 
                                file_data: bytes, filename: str, bucket_name: str, s3_key: str, region_name: str = "eu-central-1") -> Dict[str, Any]:
        """
        Uploads a file (provided as bytes) to the specified S3 bucket and key path.

        Args:
            file_data (bytes): The file content as bytes.
            filename (str): The original filename (for logging/reference).
            bucket_name (str): The target S3 bucket name.
            s3_key (str): The S3 key (path) where the file will be stored.
            region_name (str, optional): AWS region. Defaults to "eu-central-1".

        Returns:
            Dict[str, Any]: Status and message about the upload.
        """
        try:
            result = self.s3.upload_bytes(bucket_name, s3_key, file_data)
            if result.get("status") == "success":
                logger.info("Uploaded file '%s' to bucket '%s' at key '%s'.", filename, bucket_name, s3_key)
                return {
                    "status": "success",
                    "message": f"File '{filename}' uploaded to '{bucket_name}/{s3_key}'"
                }
            else:
                logger.error("Failed to upload file '%s': %s", filename, result.get("message", "Unknown error"))
                return {
                    "status": "error",
                    "message": result.get("message", "Upload failed")
                }
        except Exception as e:
            logger.error("MCP call failed: %s", str(e))
            return {
                "status": "error",
                "message": f"MCP call failed: {str(e)}"
            }


    def download_file(self, bucket: str, object_name: str) -> Dict[str, Any]:
        try:
            response = self.s3.get_object(Bucket=bucket, Key=object_name)
            file_content = response['Body'].read()  # Read the file content
            file_name = os.path.basename(object_name)  # Extract file name for download
            content_type = response.get('ContentType', 'application/octet-stream')
            
            logger.info("Downloaded file: %s from bucket: %s.", object_name, bucket)
            
            # Return file data for Streamlit download
            return {
                "status": "success",
                "file_content": file_content,
                "file_name": file_name,
                "content_type": content_type
            }
            
        except ClientError as e:
            logger.error("Error downloading file: %s from bucket: %s. Error: %s", object_name, bucket, str(e))
            return {"status": "error", "message": f"Download failed: {str(e)}"}
        except Exception as e:
            logger.error("Unexpected error: %s", str(e))
            return {"status": "error", "message": f"Unexpected error: {str(e)}"}

    def list_objects(self, bucket: str, prefix: str = "") -> Dict[str, Any]:
        try:
            response = self.s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
            objects = [obj["Key"] for obj in response.get("Contents", [])]
            return {"status": "success", "bucket": bucket, "objects": objects}
        except Exception as e:
            return format_error_response("Failed to list objects in S3 bucket", str(e))

    def delete_object(self, bucket: str, object_name: str) -> Dict[str, Any]:
        try:
            self.s3.delete_object(Bucket=bucket, Key=object_name)
            return {"status": "success", "bucket": bucket, "object_name": object_name}
        except Exception as e:
            return format_error_response("Failed to delete object from S3", str(e))
        
    