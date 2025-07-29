import os
import sys
import threading
import logging
import base64
import json
import csv
import io

import boto3
from boto3.s3.transfer import TransferConfig
from botocore.exceptions import ClientError, NoCredentialsError

from typing import Dict, List, Any, Optional
from dotenv import load_dotenv

import PyPDF2
import jsonlines


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

MB = 1024 * 1024

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

class BucketWrapper:
    """Encapsulates S3 bucket actions."""

    def __init__(self, access_key: str, secret_key: str, region_name: str = "eu-central-1"):
        """
        :param bucket: A Boto3 Bucket resource. This is a high-level resource in Boto3
                       that wraps bucket actions in a class-like structure.
        """
        self.s3 = boto3.resource(
            "s3",
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region_name
        )
        

    def create_bucket(self, bucket_name, region_name: Optional[str] = None) -> bool:
        """
        Create an S3 bucket in the specified region.

        Args:
            bucket_name (str): Name of the bucket to create
            region_name (str, optional): AWS region for the bucket. Defaults to instance region.

        Returns:
            bool: True if bucket was created or already exists, False on error
        """
        region = region_name if region_name else getattr(self, "region_name", "eu-west-2")
        try:
            # Check if bucket already exists
            self.s3.meta.client.head_bucket(Bucket=bucket_name)
            logger.info(f"Bucket '{bucket_name}' already exists")
            return True
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '404':
                # Bucket doesn't exist, create it
                try:
                    self.s3.create_bucket(
                        Bucket=bucket_name,
                        CreateBucketConfiguration={'LocationConstraint': region}
                    )
                    logger.info(f"Successfully created bucket '{bucket_name}' in region '{region}'")
                    return True
                except ClientError as create_error:
                    logger.error(f"Failed to create bucket '{bucket_name}': {str(create_error)}")
                    return False
            else:
                logger.error(f"Error checking bucket '{bucket_name}': {str(e)}")
                return False
        
    def list(self):
        """
        Get the buckets in all Regions for the current account.

        :param s3_resource: A Boto3 S3 resource. This is a high-level resource in Boto3
                            that contains collections and factory methods to create
                            other high-level S3 sub-resources.
        :return: The list of buckets.
        """
        try:
            buckets = list(self.s3.buckets.all())
            logger.info("Got buckets: %s.", buckets)
        except ClientError:
            logger.exception("Couldn't get buckets.")
            raise
        else:
            return buckets


class TransferCallback:
    """
    Handle callbacks from the transfer manager.

    The transfer manager periodically calls the __call__ method throughout
    the upload and download process so that it can take action, such as
    displaying progress to the user and collecting data about the transfer.
    """

    def __init__(self, target_size, access_key: str, secret_key: str, region_name: str = "eu-central-1"):  
        self.s3 = boto3.resource(
            "s3",
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region_name
        )
        self._target_size = target_size
        self._total_transferred = 0
        self._lock = threading.Lock()
        self.thread_info = {}

    def __call__(self, bytes_transferred):
        """
        The callback method that is called by the transfer manager.

        Display progress during file transfer and collect per-thread transfer
        data. This method can be called by multiple threads, so shared instance
        data is protected by a thread lock.
        """
        thread = threading.current_thread()
        with self._lock:
            self._total_transferred += bytes_transferred
            if thread.ident not in self.thread_info.keys():
                self.thread_info[thread.ident] = bytes_transferred
            else:
                self.thread_info[thread.ident] += bytes_transferred

            target = self._target_size * MB
            sys.stdout.write(
                f"\r{self._total_transferred} of {target} transferred "
                f"({(self._total_transferred / target) * 100:.2f}%)."
            )
            sys.stdout.flush()


    def upload_with_default_configuration(self,
        local_file_path, bucket_name, object_key, file_size_mb
    ):
        """
        Upload a file from a local folder to an Amazon S3 bucket, using the default
        configuration.
        """
        transfer_callback = TransferCallback(file_size_mb)
        self.s3.Bucket(bucket_name).upload_file(
            local_file_path, object_key, Callback=transfer_callback
        )
        return transfer_callback.thread_info


    def upload_with_chunksize_and_meta(self,
        local_file_path, bucket_name, object_key, file_size_mb, metadata=None
    ):
        """
        Upload a file from a local folder to an Amazon S3 bucket, setting a
        multipart chunk size and adding metadata to the Amazon S3 object.

        The multipart chunk size controls the size of the chunks of data that are
        sent in the request. A smaller chunk size typically results in the transfer
        manager using more threads for the upload.

        The metadata is a set of key-value pairs that are stored with the object
        in Amazon S3.
        """
        transfer_callback = TransferCallback(file_size_mb)

        config = TransferConfig(multipart_chunksize=1 * MB)
        extra_args = {"Metadata": metadata} if metadata else None
        self.s3.Bucket(bucket_name).upload_file(
            local_file_path,
            object_key,
            Config=config,
            ExtraArgs=extra_args,
            Callback=transfer_callback,
        )
        return transfer_callback.thread_info


    def upload_with_high_threshold(self, local_file_path, bucket_name, object_key, file_size_mb):
        """
        Upload a file from a local folder to an Amazon S3 bucket, setting a
        multipart threshold larger than the size of the file.

        Setting a multipart threshold larger than the size of the file results
        in the transfer manager sending the file as a standard upload instead of
        a multipart upload.
        """
        transfer_callback = TransferCallback(file_size_mb)
        config = TransferConfig(multipart_threshold=file_size_mb * 2 * MB)
        self.s3.Bucket(bucket_name).upload_file(
            local_file_path, object_key, Config=config, Callback=transfer_callback
        )
        return transfer_callback.thread_info


    def upload_with_sse(self,
        local_file_path, bucket_name, object_key, file_size_mb, sse_key=None
    ):
        """
        Upload a file from a local folder to an Amazon S3 bucket, adding server-side
        encryption with customer-provided encryption keys to the object.

        When this kind of encryption is specified, Amazon S3 encrypts the object
        at rest and allows downloads only when the expected encryption key is
        provided in the download request.
        """
        transfer_callback = TransferCallback(file_size_mb)
        if sse_key:
            extra_args = {"SSECustomerAlgorithm": "AES256", "SSECustomerKey": sse_key}
        else:
            extra_args = None
        self.s3.Bucket(bucket_name).upload_file(
            local_file_path, object_key, ExtraArgs=extra_args, Callback=transfer_callback
        )
        return transfer_callback.thread_info


    def download_with_default_configuration(self,
        bucket_name, object_key, download_file_path, file_size_mb
    ):
        """
        Download a file from an Amazon S3 bucket to a local folder, using the
        default configuration.
        """
        transfer_callback = TransferCallback(file_size_mb)
        self.s3.Bucket(bucket_name).Object(object_key).download_file(
            download_file_path, Callback=transfer_callback
        )
        return transfer_callback.thread_info


    def download_with_single_thread(self,
        bucket_name, object_key, download_file_path, file_size_mb
    ):
        """
        Download a file from an Amazon S3 bucket to a local folder, using a
        single thread.
        """
        transfer_callback = TransferCallback(file_size_mb)
        config = TransferConfig(use_threads=False)
        self.s3.Bucket(bucket_name).Object(object_key).download_file(
            download_file_path, Config=config, Callback=transfer_callback
        )
        return transfer_callback.thread_info


    def download_with_high_threshold(self,
        bucket_name, object_key, download_file_path, file_size_mb
    ):
        """
        Download a file from an Amazon S3 bucket to a local folder, setting a
        multipart threshold larger than the size of the file.

        Setting a multipart threshold larger than the size of the file results
        in the transfer manager sending the file as a standard download instead
        of a multipart download.
        """
        transfer_callback = TransferCallback(file_size_mb)
        config = TransferConfig(multipart_threshold=file_size_mb * 2 * MB)
        self.s3.Bucket(bucket_name).Object(object_key).download_file(
            download_file_path, Config=config, Callback=transfer_callback
        )
        return transfer_callback.thread_info


    def download_with_sse(self,
        bucket_name, object_key, download_file_path, file_size_mb, sse_key
    ):
        """
        Download a file from an Amazon S3 bucket to a local folder, adding a
        customer-provided encryption key to the request.

        When this kind of encryption is specified, Amazon S3 encrypts the object
        at rest and allows downloads only when the expected encryption key is
        provided in the download request.
        """
        transfer_callback = TransferCallback(file_size_mb)

        if sse_key:
            extra_args = {"SSECustomerAlgorithm": "AES256", "SSECustomerKey": sse_key}
        else:
            extra_args = None
        self.s3.Bucket(bucket_name).Object(object_key).download_file(
            download_file_path, ExtraArgs=extra_args, Callback=transfer_callback
        )
        return transfer_callback.thread_info
    


class S3FileDownloader:
    def __init__(self, access_key: str, secret_key: str, region_name: str = "eu-central-1"):
        try:
            # Initialize S3 client
            self.s3 = boto3.client(
            "s3",
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region_name
        )
        except NoCredentialsError:
            logger.error("AWS credentials not found")
            raise
    
    def download_file_stream(self, bucket: str, object_name: str) -> Dict[str, Any]:
        """
        Description:
            Downloads a file from an S3 bucket and returns its contents as a stream, along with metadata for browser download.
        Args:
            bucket (str): The name of the S3 bucket.
            object_name (str): The key (path) of the object in the S3 bucket.
        Returns:
            Dict[str, Any]: A dictionary containing:
                - status (str): "success" if the file was downloaded, otherwise "error".
                - file_stream: The file's content as a stream (for browser download).
                - file_name (str): The name of the file.
                - content_type (str): The MIME type of the file.
                - file_size (int): The size of the file in bytes.
                - message (str, optional): Error message if the download fails.
        Raises:
            Logs errors and returns error information in the response dictionary if the file or bucket is not found, or if an unexpected error occurs.
        """
        """Download file from S3 and return as stream for browser download"""
        try:
            # Get object metadata first
            response = self.s3.head_object(Bucket=bucket, Key=object_name)
            file_size = response['ContentLength']
            content_type = response.get('ContentType', 'application/octet-stream')
            
            # Get the actual file
            file_response = self.s3.get_object(Bucket=bucket, Key=object_name)
            file_stream = file_response['Body']
            
            # Extract filename from object key
            file_name = os.path.basename(object_name)
            
            logger.info(f"Successfully retrieved file: {object_name} from bucket: {bucket}")
            
            return {
                "status": "success",
                "file_stream": file_stream,
                "file_name": file_name,
                "content_type": content_type,
                "file_size": file_size
            }
            
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'NoSuchKey':
                logger.error(f"File not found: {object_name} in bucket: {bucket}")
                return {"status": "error", "message": f"File '{object_name}' not found in bucket '{bucket}'"}
            elif error_code == 'NoSuchBucket':
                logger.error(f"Bucket not found: {bucket}")
                return {"status": "error", "message": f"Bucket '{bucket}' not found"}
            else:
                logger.error(f"Error downloading file: {object_name} from bucket: {bucket}. Error: {str(e)}")
                return {"status": "error", "message": f"Download failed: {str(e)}"}
        except Exception as e:
            logger.error(f"Unexpected error: {str(e)}")
            return {"status": "error", "message": f"Unexpected error: {str(e)}"}
    
    def generate_presigned_url(self, bucket: str, object_name: str, expiration: int = 3600) -> Dict[str, Any]:
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
        Returns:
            Dict[str, Any]: A dictionary containing the status, the presigned download URL, expiration time, and file name on success.
                            On failure, returns a dictionary with status "error" and an error message.
        """
        """Generate a presigned URL for downloading file from S3"""
        try:
            url = self.s3.generate_presigned_url(
                'get_object',
                Params={'Bucket': bucket, 'Key': object_name},
                ExpiresIn=expiration
            )
            
            logger.info(f"Generated presigned URL for: {object_name} from bucket: {bucket}")
            
            return {
                "status": "success",
                "download_url": url,
                "expires_in": expiration,
                "file_name": os.path.basename(object_name)
            }
            
        except ClientError as e:
            logger.error(f"Error generating presigned URL: {str(e)}")
            return {"status": "error", "message": f"Failed to generate download URL: {str(e)}"}
        
    def read_file_from_s3(self, bucket: str, object_name: str) -> Dict[str, Any]:
        """
        Read file from S3 and process based on file extension.
        
        Args:
            bucket (str): S3 bucket name
            object_name (str): S3 object key
            
        Returns:
            Dict containing status and data or error message
        """
        try:
            # Get file from S3
            response = self.s3.get_object(Bucket=bucket, Key=object_name)
            file_content = response['Body'].read()
            
            # Determine file type from extension
            file_extension = object_name.lower().split('.')[-1]
            
            if file_extension == 'csv':
                data = self._parse_csv(file_content)
            elif file_extension == 'json':
                data = self._parse_json(file_content)
            elif file_extension == 'jsonl':
                data = self._parse_jsonl(file_content)
            elif file_extension in ('txt', 'md'):
                data = self._parse_text(file_content)
            elif file_extension == 'pdf':
                data = self._parse_pdf(file_content)
            else:
                return {
                    "status": "error",
                    "message": f"Unsupported file type: {file_extension}"
                }
                
            return {
                "status": "success",
                "data": data
            }
            
        except ClientError as e:
            return {
                "status": "error",
                "message": f"S3 error: {str(e)}"
            }
        except Exception as e:
            return {
                "status": "error",
                "message": f"Error processing file: {str(e)}"
            }

    def _parse_csv(self, content: bytes) -> list:
        """Parse CSV content to list of dictionaries."""
        text = content.decode('utf-8')
        csv_file = io.StringIO(text)
        return list(csv.DictReader(csv_file))

    def _parse_json(self, content: bytes) -> Any:
        """Parse JSON content."""
        return json.loads(content.decode('utf-8'))

    def _parse_jsonl(self, content: bytes) -> list:
        """Parse JSONL content to list of dictionaries."""
        result = []
        with io.BytesIO(content) as file:
            reader = jsonlines.Reader(file)
            for obj in reader:
                result.append(obj)
        return result

    def _parse_text(self, content: bytes) -> str:
        """Parse text or markdown content."""
        return content.decode('utf-8')

    def _parse_pdf(self, content: bytes) -> list:
        """Parse PDF content to list of page texts."""
        pdf_file = io.BytesIO(content)
        pdf_reader = PyPDF2.PdfReader(pdf_file)
        return [page.extract_text() for page in pdf_reader.pages]


