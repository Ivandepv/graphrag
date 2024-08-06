import logging
import re
from collections.abc import Iterator
from typing import Any, cast

import boto3
from botocore.exceptions import ClientError
from datashaper import Progress

from graphrag.index.progress import ProgressReporter

from .typing import PipelineStorage

log = logging.getLogger(__name__)

class S3PipelineStorage(PipelineStorage):
    """S3 storage class definition."""

    def __init__(self,
                 aws_access_key_id: str | None = None, 
                 aws_secret_access_key: str | None = None, 
                 bucket_name: str | None = None,
                 base_prefix: str | None = None, 
                 region_name: str | None = None):
        """Init method definition."""
        self.bucket_name = bucket_name
        print(f"bucket_name: {bucket_name}")
        self.base_prefix = base_prefix or ""
        print(f"base_prefix: {base_prefix}")
        self.s3 = boto3.client("s3")

    def find(
        self,
        file_pattern: re.Pattern[str],
        base_dir: str | None = None,
        progress: ProgressReporter | None = None,
        file_filter: dict[str, Any] | None = None,
        max_count=-1,
    ) -> Iterator[tuple[str, dict[str, Any]]]:
        """Find files in the S3 bucket using a file pattern, as well as a custom filter function."""
        search_prefix = f"{self.base_prefix}/{base_dir}" if base_dir else self.base_prefix
        paginator = self.s3.get_paginator('list_objects_v2')
        
        num_loaded = 0
        num_filtered = 0
        num_total = 0

        for page in paginator.paginate(Bucket=self.bucket_name, Prefix=search_prefix):
            for obj in page.get('Contents', []):
                num_total += 1
                key = obj['Key']
                match = file_pattern.match(key)
                if match:
                    group = match.groupdict()
                    if file_filter is None or all(re.match(value, group[key]) for key, value in file_filter.items()):
                        yield (key, group)
                        num_loaded += 1
                        if max_count > 0 and num_loaded >= max_count:
                            return
                    else:
                        num_filtered += 1
                else:
                    num_filtered += 1

                if progress is not None:
                    progress(_create_progress_status(num_loaded, num_filtered, num_total))

    async def get(self, key: str, as_bytes: bool | None = False, encoding: str | None = None) -> Any:
        """Get method definition."""
        full_key = f"{self.base_prefix}/{key}" if self.base_prefix else key
        # Temporal solution s3_text
         # Check for duplicate base prefix and adjust if necessary
        if full_key.count(self.base_prefix) > 1:
            full_key = full_key.replace(f"{self.base_prefix}/", "", 1)
        try:
            response = self.s3.get_object(Bucket=self.bucket_name, Key=full_key)
            data = response['Body'].read()
            if as_bytes:
                return data
            return data.decode(encoding or 'utf-8')
        except ClientError as e:
            log.error(f"Error fetching object from S3: {e.response['Error']['Message']}")
            print(f"Error fetching object from S3: {e.response['Error']['Message']}")
            if e.response['Error']['Code'] == 'NoSuchKey':

                return None
            raise

    async def set(self, key: str, value: Any, encoding: str | None = None) -> None:
        """Set method definition."""
        full_key = f"{self.base_prefix}/{key}" if self.base_prefix else key
        if isinstance(value, bytes):
            self.s3.put_object(Bucket=self.bucket_name, Key=full_key, Body=value)
        else:
            encoded_value = value.encode(encoding or 'utf-8') if isinstance(value, str) else value
            self.s3.put_object(Bucket=self.bucket_name, Key=full_key, Body=encoded_value)

    async def has(self, key: str) -> bool:
        """Has method definition."""
        full_key = f"{self.base_prefix}/{key}" if self.base_prefix else key
        try:
            self.s3.head_object(Bucket=self.bucket_name, Key=full_key)
            return True
        except ClientError:
            return False

    async def delete(self, key: str) -> None:
        """Delete method definition."""
        full_key = f"{self.base_prefix}/{key}" if self.base_prefix else key
        self.s3.delete_object(Bucket=self.bucket_name, Key=full_key)

    async def clear(self) -> None:
        """Clear method definition."""
        paginator = self.s3.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=self.bucket_name, Prefix=self.base_prefix):
            delete_keys = {'Objects': [{'Key': obj['Key']} for obj in page.get('Contents', [])]}
            if delete_keys['Objects']:
                self.s3.delete_objects(Bucket=self.bucket_name, Delete=delete_keys)

    def child(self, name: str | None) -> "PipelineStorage":
        """Create a child storage instance."""
        if name is None:
            return self
        new_prefix = f"{self.base_prefix}/{name}" if self.base_prefix else name
        return S3PipelineStorage(
            aws_access_key_id=self.s3.meta.client.meta.config.credentials.access_key,
            aws_secret_access_key=self.s3.meta.client.meta.config.credentials.secret_key,
            bucket_name=self.bucket_name,
            base_prefix=new_prefix,
            region_name=self.s3.meta.client.meta.region_name
        )

def create_s3_storage(
    
        aws_access_key_id: str | None = None, 
        aws_secret_access_key: str | None = None, 
        bucket_name: str | None=None, 
        base_prefix: str | None = None, 
                      region_name: str | None = None) -> PipelineStorage:
    """Create an S3 based storage."""
    log.info("Creating S3 storage in bucket %s with prefix %s", bucket_name, base_prefix)
    return S3PipelineStorage(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        bucket_name=bucket_name,
        base_prefix=base_prefix,
        region_name=region_name
    )

def _create_progress_status(num_loaded: int, num_filtered: int, num_total: int) -> Progress:
    return Progress(
        total_items=num_total,
        completed_items=num_loaded + num_filtered,
        description=f"{num_loaded} files loaded ({num_filtered} filtered)",
    )
