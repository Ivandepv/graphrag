# Copyright (c) 2024 Microsoft Corporation.
# Licensed under the MIT License
"""A module containing load method definition for S3 text input."""

import logging
import re
from pathlib import Path
from typing import Any

import pandas as pd
import boto3
from botocore.exceptions import ClientError

from graphrag.index.config import PipelineInputConfig
from graphrag.index.progress import ProgressReporter
from graphrag.index.utils import gen_md5_hash

input_type = "s3_text"
log = logging.getLogger(__name__)

DEFAULT_FILE_PATTERN = re.compile(
    r".*[\\/](?P<source>[^\\/]+)[\\/](?P<year>\d{4})-(?P<month>\d{2})-(?P<day>\d{2})_(?P<author>[^_]+)_\d+\.txt"
)

async def load(
    config: PipelineInputConfig,
    progress: ProgressReporter | None,
    _storage: Any,  # We don't use the storage parameter for S3
) -> pd.DataFrame:
    """Load text inputs from an S3 bucket."""
    if not config.bucket_name or not config.base_prefix:
        raise ValueError("S3 bucket name and base prefix are required for S3 input")

    s3 = boto3.client('s3')

    async def load_file(key: str, group: dict | None = None) -> dict[str, Any]:
        if group is None:
            group = {}
        try:
            obj = s3.get_object(Bucket=config.bucket_name, Key=key)
            text = obj['Body'].read().decode('utf-8')
            new_item = {**group, "text": text}
            new_item["id"] = gen_md5_hash(new_item, new_item.keys())
            new_item["title"] = str(Path(key).name)
            return new_item
        except ClientError as e:
            log.error(f"Error reading file {key} from S3: {e}")
            raise

    file_pattern = re.compile(config.file_pattern or DEFAULT_FILE_PATTERN)

    try:
        paginator = s3.get_paginator('list_objects_v2')
        files = []
        for page in paginator.paginate(Bucket=config.bucket_name, Prefix=config.base_prefix):
            for obj in page.get('Contents', []):
                key = obj['Key']
                if key.endswith('.txt'):
                    match = file_pattern.match(key)
                    if match:
                        group = match.groupdict()
                        files.append((key, group))

        if len(files) == 0:
            msg = f"No text files found in S3 bucket {config.bucket_name} with prefix {config.base_prefix}"
            raise ValueError(msg)

        log.info(f"Found {len(files)} text files in S3 bucket {config.bucket_name}")

        files_loaded = []
        for file, group in files:
            if progress:
                progress.report_progress(f"Loading file: {file}")
            try:
                files_loaded.append(await load_file(file, group))
            except Exception:
                log.warning(f"Warning! Error loading file {file} from S3. Skipping...")

        log.info(f"Found {len(files)} files, loading {len(files_loaded)}")
        return pd.DataFrame(files_loaded)

    except ClientError as e:
        log.error(f"Error accessing S3: {e}")
        raise