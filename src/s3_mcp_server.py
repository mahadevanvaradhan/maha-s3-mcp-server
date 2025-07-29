import os
import platform
import requests
from src.s3_utils.s3_functions import S3Client
from src.s3_utils.s3_file_transfer import BucketWrapper, S3FileDownloader
from typing import Dict, List, Any, Optional
from dotenv import load_dotenv
from mcp.server.fastmcp import FastMCP
import logging
import sys
import pickle
import re
import base64


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")
MCP_SERVER_PORT = os.getenv("S3_MCP_SERVER_PORT", "8002")
MCP_SERVER_URL = f"http://localhost:{MCP_SERVER_PORT}"

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

def extract_s3_path_from_message(message: str) -> Optional[tuple[str, str]]:
    """
    Extract S3 bucket and key from various chat message formats
    
    Returns:
        Tuple[bucket_name, s3_key] or None if not found
    """
    # Common patterns for S3 paths in chat messages
    patterns = [
        r's3://([^/\s]+)/([^\s]+)',  # s3://bucket/key/path
        r'to\s+s3://([^/\s]+)/([^\s]+)',  # "to s3://bucket/key"
        r'upload.*?to\s+s3://([^/\s]+)/([^\s]+)',  # "upload file to s3://bucket/key"
        r'save.*?to\s+s3://([^/\s]+)/([^\s]+)',  # "save to s3://bucket/key"
        r'store.*?at\s+s3://([^/\s]+)/([^\s]+)',  # "store at s3://bucket/key"
        r'put.*?in\s+s3://([^/\s]+)/([^\s]+)',  # "put in s3://bucket/key"
        r's3://([^/\s]+)/?$',  # just s3://bucket/ (will need filename appended)
    ]
    
    for pattern in patterns:
        match = re.search(pattern, message, re.IGNORECASE)
        if match:
            bucket_name = match.group(1).strip()
            s3_key = match.group(2).strip() if len(match.groups()) > 1 else ""
            return bucket_name, s3_key
    
    return None

def infer_s3_key_if_missing(s3_key: str, filename: str) -> str:
    """
    If s3_key is empty or ends with /, append the filename
    """
    if not s3_key or s3_key.endswith('/'):
        return s3_key + filename
    return s3_key

mcp = FastMCP(
    "AWS S3 MCP Server",
    description="A server to handle AWS S3 operations with chat-based file upload support.",
    version="1.0.0",
    author="Mahadevan",
    instructions="S3 API server to handle file uploads, downloads, and other utilities with natural language chat interface.",
    debug=False,
    log_level="INFO",
    host="s3-mcp-server", # Use the Docker service name for internal communication
    port=int(MCP_SERVER_PORT) 
)

# Custom Function 1
@mcp.tool()
def s3_create(bucket: str, region_name: Optional[str] = None) -> Dict[str, Any]:
    """
    Description: Creates an S3 bucket in the specified region.
                This function attempts to create a new S3 bucket with the given name and optional region.
                If the bucket creation is successful, an informational log is recorded. If an error occurs,
                the function returns an error message.
    Args:
        bucket (str): The name of the bucket to create.
        region_name (Optional[str], optional): The AWS region where the bucket should be created. Defaults to None.
    Returns:
        Dict[str, Any]: Information about the created bucket or an error message if creation fails.
    """
    try:
        s3_resource = BucketWrapper(AWS_ACCESS_KEY, AWS_SECRET_KEY, region_name)
        s3_resource.create_bucket(bucket, region_name)
        logger.info("Created bucket: %s.", bucket)
        return {"status": "success", "message": f"Bucket '{bucket}' created successfully"}
    except requests.exceptions.RequestException as e:
        return {"status": "error", "message": f"Create Bucket Failed: {str(e)}"}
        
# Custom Function 2
@mcp.tool()
def s3_list_bucket(region_name: Optional[str] = None):
    """
    Description:
    Lists all S3 buckets across all regions for the current AWS account.

    This function uses the Boto3 S3 resource to retrieve the list of buckets available to the current account.
    It logs the retrieved bucket information and handles any request exceptions that may occur during the process.

    Returns:
        Dict[str, Any]: A dictionary containing the list of buckets if successful, or an error message if the request fails.
    """
    try:
        s3_client = S3Client(AWS_ACCESS_KEY, AWS_SECRET_KEY, region_name)
        all_buckets = s3_client.list_buckets()
        logger.info("List buckets: %s.", all_buckets)
        return all_buckets
    except requests.exceptions.RequestException as e:
        return {"status": "error", "message": f"List Bucket Failed: {str(e)}"}

# Custom Function 3
@mcp.tool()
def s3_list_object_from_bucket(bucket: str, prefix: str = "", region_name: str = "eu-central-1") -> Dict[str, Any]:
    """
    Description:
        This function interacts with the S3 client to retrieve a list of objects from the specified bucket.
        It supports filtering objects by a given prefix. Handles network and response errors gracefully.
    Args:
        bucket (str): The name of the S3 bucket to list objects from.
        prefix (str, optional): The prefix to filter objects by. Defaults to "".
        region_name (str, optional): AWS region. Defaults to "eu-central-1".
    Returns:
        Dict[str, Any]: A dictionary containing the listed objects if successful.
        str: An error message if the request fails or the response is malformed.
    """
    try:
        s3_client = S3Client(AWS_ACCESS_KEY, AWS_SECRET_KEY, region_name)
        objects = s3_client.list_objects(bucket, prefix)
        logger.info("Got objects: %s.", objects)
        return objects
    except requests.exceptions.RequestException as e:
        return {"status": "error", "message": f"Unable to list S3 objects: {str(e)}"}


# Custom Function 4  
@mcp.tool()
def s3_download_file_presigned_url(bucket: str, object_name: str, expiration: int = 3600, region_name: str = "eu-central-1") -> Dict[str, Any]:
    """
    Descriptions:
        Generates a presigned URL for downloading a file from an AWS S3 bucket.
        This function creates a temporary, secure URL that allows users to download a specific object from the specified S3 bucket.
        The URL is valid for a limited time, defined by the `expiration` parameter. This is useful for granting time-limited access
        to private files stored in S3 without exposing AWS credentials.
    Args:
        bucket (str): The name of the S3 bucket containing the file.
        object_name (str): The key (path) of the file within the S3 bucket.
        expiration (int, optional): Time in seconds for which the presigned URL is valid. Defaults to 3600 (1 hour).
        region_name (str, optional): AWS region. Defaults to "eu-central-1".
    Returns:
        Dict[str, Any]: A dictionary containing the status, the presigned download URL, expiration time, and file name on success.
                        On failure, returns a dictionary with status "error" and an error message.
    """
    try:
        # Validate required parameters
        if not bucket or not object_name:
            return {
                "status": "error",
                "message": "Both 'bucket' and 'object_name' parameters are required"
            }
        
        # Generate presigned URL with expiration parameter
        s3_download = S3FileDownloader(AWS_ACCESS_KEY, AWS_SECRET_KEY, region_name)
        result = s3_download.generate_presigned_url(bucket, object_name, expiration)
        
        if result["status"] == "error":
            logger.error("Failed to generate presigned URL for file: %s from bucket: %s. Error: %s", 
                        object_name, bucket, result["message"])
            return result
        
        logger.info("Generated presigned URL for file: %s from bucket: %s.", object_name, bucket)
        
        # Return the result directly (don't use jsonify in MCP tools)
        return result
        
    except Exception as e:
        logger.error("Unexpected error generating presigned URL: %s", str(e))
        return {
            "status": "error",
            "message": f"Internal server error: {str(e)}"
        }
    

# Custom Function 5
@mcp.tool()
def s3_read_file(bucket: str, object_name: str, region_name: str = "eu-central-1") -> Dict[str, Any]:
    """
    Description: Reads a file from an S3 bucket and returns its contents.
    This function validates the required parameters, attempts to read the specified object from the given S3 bucket using a downloader utility, and handles errors gracefully. It logs relevant information and errors, and returns a dictionary containing the status and result or error message.
    Args:
        bucket (str): The name of the S3 bucket.
        object_name (str): The key (name) of the object to read from the bucket.
        region_name (str, optional): The AWS region where the bucket is located. Defaults to "eu-central-1".
    Returns:
        Dict[str, Any]: A dictionary containing the status ("success" or "error") and either the file data or an error message.
    """
    try:
        # Validate required parameters
        if not bucket or not object_name:
            return {
                "status": "error",
                "message": "Both 'bucket' and 'object_name' parameters are required"
            }
        
        s3_read = S3FileDownloader(AWS_ACCESS_KEY, AWS_SECRET_KEY, region_name)
        result = s3_read.read_file_from_s3(bucket, object_name)
        
        if result["status"] == "error":
            logger.error("Failed to read from file: %s from bucket: %s. Error: %s", 
                        object_name, bucket, result["message"])
            return result
        
        logger.info("Read data from file: %s from bucket: %s.", object_name, bucket)
        
        return result
        
    except Exception as e:
        logger.error("Unexpected error reading file: %s", str(e))
        return {
            "status": "error",
            "message": f"Internal server error: {str(e)}"
        }

if __name__ == "__main__":
    mcp.run(transport="sse")