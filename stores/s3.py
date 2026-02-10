"""S3-compatible storage backend (AWS S3, Cloudflare R2, MinIO, etc.)."""

from __future__ import annotations

import logging
import os

import boto3
from botocore.config import Config as BotoConfig

from config import ConfigError

from . import BackupInfo, Store, is_backup_file, parse_timestamp

log = logging.getLogger(__name__)


class S3Store(Store):
    def __init__(
        self,
        bucket: str,
        endpoint: str | None = None,
        access_key: str = "",
        secret_key: str = "",
        region: str = "auto",
    ):
        session = boto3.session.Session(
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
        )
        client_kwargs: dict = {
            "config": BotoConfig(
                signature_version="s3v4",
                retries={"max_attempts": 3, "mode": "standard"},
            ),
            "region_name": region,
        }
        if endpoint:
            client_kwargs["endpoint_url"] = endpoint

        self._client = session.client("s3", **client_kwargs)
        self._bucket = bucket

    def upload(self, local_path: str, remote_key: str) -> None:
        log.info("Uploading %s -> s3://%s/%s", local_path, self._bucket, remote_key)
        local_size = os.path.getsize(local_path)
        self._client.upload_file(local_path, self._bucket, remote_key)

        # Verify uploaded object size matches the local file
        resp = self._client.head_object(Bucket=self._bucket, Key=remote_key)
        remote_size = resp["ContentLength"]
        if remote_size != local_size:
            raise RuntimeError(
                f"Upload verification failed for '{remote_key}': "
                f"local size {local_size} != remote size {remote_size}"
            )

    def download(self, remote_key: str, local_path: str) -> None:
        log.info(
            "Downloading s3://%s/%s -> %s", self._bucket, remote_key, local_path
        )
        self._client.download_file(self._bucket, remote_key, local_path)

    def list(self, prefix: str) -> list[BackupInfo]:
        backups: list[BackupInfo] = []
        paginator = self._client.get_paginator("list_objects_v2")

        for page in paginator.paginate(Bucket=self._bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                filename = key.rsplit("/", 1)[-1]
                if not is_backup_file(filename):
                    continue
                ts = parse_timestamp(filename)
                if ts is None:
                    continue
                backups.append(
                    BackupInfo(
                        key=key,
                        filename=filename,
                        timestamp=ts,
                        size=obj.get("Size", 0),
                    )
                )

        backups.sort(key=lambda b: b.timestamp)
        return backups

    def delete(self, remote_key: str) -> None:
        log.info("Deleting s3://%s/%s", self._bucket, remote_key)
        self._client.delete_object(Bucket=self._bucket, Key=remote_key)


def create(config: dict) -> S3Store:
    if "bucket" not in config:
        raise ConfigError("Error: S3 store config is missing required 'bucket' field")
    return S3Store(
        bucket=config["bucket"],
        endpoint=config.get("endpoint"),
        access_key=config.get("access_key", ""),
        secret_key=config.get("secret_key", ""),
        region=config.get("region", "auto"),
    )
