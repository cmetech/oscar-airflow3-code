"""TLS-only S3 access to OSCAR object storage using an Airflow connection."""
from functools import cached_property
from urllib.parse import urlsplit

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.utils.connection_wrapper import AwsConnectionWrapper
from botocore.config import Config
from helpers.file_store import FileStore


class ObjectStorageHook(FileStore, S3Hook):
    """Use scoped credentials; instantiate inside tasks, never during DAG parsing.

    Inherits S3Hook's load_bytes/load_string/read_key/download_file and listing
    methods. upload_file returns a small reference suitable for downstream XCom.
    """

    def __init__(self, conn_id="oscar_object_storage", bucket_name=None):
        if not conn_id:
            raise ValueError("An explicit object-storage connection is required")
        self.bucket_name = bucket_name
        super().__init__(aws_conn_id=conn_id, config=Config(
            signature_version="s3v4", s3={"addressing_style": "path"},
            connect_timeout=10, read_timeout=60,
            retries={"mode": "standard", "max_attempts": 3},
        ))

    @cached_property
    def conn_config(self):
        # Do not inherit the provider's missing-connection fallback to ambient AWS credentials.
        connection = self.get_connection(self.aws_conn_id)
        if connection.conn_type != "aws":
            raise ValueError("Object-storage connection must have type aws")
        config = AwsConnectionWrapper(conn=connection, botocore_config=self._config)
        endpoint = urlsplit(config.get_service_endpoint_url("s3") or "")
        if (endpoint.scheme != "https" or not endpoint.hostname or endpoint.username
                or endpoint.password or endpoint.path not in ("", "/") or endpoint.query or endpoint.fragment):
            raise ValueError("Object storage requires an explicit HTTPS endpoint origin")
        if not config.aws_access_key_id or not config.aws_secret_access_key or config.role_arn or config.profile_name:
            raise ValueError("Object storage requires explicit scoped access and secret keys, without AWS roles/profiles")
        if config.verify is None:
            config.verify = "/opt/airflow/ssl/ca-cert.pem"
        if config.verify is not True and not (isinstance(config.verify, str) and config.verify.strip()):
            raise ValueError("Object-storage TLS verification cannot be disabled")
        config.region_name = config.region_name or "us-east-1"
        return config

    def upload_file(self, filename, bucket_name, key, *, replace=False):
        """Upload a local file and return only its location; overwrite is opt-in.

        Use run-specific keys. The provider's replace=False check is not an
        atomic conditional write and cannot arbitrate concurrent writers.
        """
        self.load_file(filename=filename, bucket_name=bucket_name, key=key, replace=replace)
        return {"bucket": bucket_name, "key": key}

    def _file_client(self):
        return self.get_conn()

    def _file_bucket(self):
        bucket = self.bucket_name or self.conn_config.extra_config.get("bucket_name")
        if not isinstance(bucket, str) or not bucket:
            raise ValueError("Configure bucket_name in the object-storage connection extras")
        return bucket
