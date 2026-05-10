"""Thin S3 wrapper: upload, download, list, and presign operations."""
from __future__ import annotations

import os
from pathlib import Path
from typing import Iterator

import boto3
import structlog
from botocore.exceptions import ClientError

logger = structlog.get_logger(__name__)


class S3Client:
    def __init__(
        self,
        bucket: str,
        region: str | None = None,
        endpoint_url: str | None = None,
    ) -> None:
        self.bucket = bucket
        self._s3 = boto3.client(
            "s3",
            region_name=region or os.environ.get("AWS_DEFAULT_REGION", "us-west-2"),
            endpoint_url=endpoint_url,
        )

    def upload_file(self, local_path: str, s3_key: str) -> str:
        self._s3.upload_file(local_path, self.bucket, s3_key)
        s3_uri = f"s3://{self.bucket}/{s3_key}"
        logger.info("s3_upload", local=local_path, uri=s3_uri)
        return s3_uri

    def upload_bytes(self, data: bytes, s3_key: str, content_type: str = "application/octet-stream") -> str:
        self._s3.put_object(Bucket=self.bucket, Key=s3_key, Body=data, ContentType=content_type)
        return f"s3://{self.bucket}/{s3_key}"

    def download_file(self, s3_key: str, local_path: str) -> None:
        Path(local_path).parent.mkdir(parents=True, exist_ok=True)
        self._s3.download_file(self.bucket, s3_key, local_path)
        logger.info("s3_download", key=s3_key, local=local_path)

    def list_keys(self, prefix: str, suffix: str = "") -> Iterator[str]:
        paginator = self._s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if key.endswith(suffix):
                    yield key

    def delete_key(self, s3_key: str) -> None:
        self._s3.delete_object(Bucket=self.bucket, Key=s3_key)

    def presign_url(self, s3_key: str, expiry_seconds: int = 3600) -> str:
        return self._s3.generate_presigned_url(
            "get_object",
            Params={"Bucket": self.bucket, "Key": s3_key},
            ExpiresIn=expiry_seconds,
        )

    def key_exists(self, s3_key: str) -> bool:
        try:
            self._s3.head_object(Bucket=self.bucket, Key=s3_key)
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                return False
            raise

    def ensure_bucket(self, region: str = "us-west-2") -> None:
        try:
            self._s3.head_bucket(Bucket=self.bucket)
        except ClientError as e:
            if e.response["Error"]["Code"] in ("404", "NoSuchBucket"):
                if region == "us-east-1":
                    self._s3.create_bucket(Bucket=self.bucket)
                else:
                    self._s3.create_bucket(
                        Bucket=self.bucket,
                        CreateBucketConfiguration={"LocationConstraint": region},
                    )
                logger.info("bucket_created", bucket=self.bucket)
            else:
                raise
