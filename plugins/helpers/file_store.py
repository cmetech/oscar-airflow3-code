"""File-reference protocol shared by OSCAR task helpers and Airflow hooks.

Keep this module byte-identical in tm_credentials and Airflow plugins/helpers.
"""
from contextlib import contextmanager
import os
from pathlib import Path
import re
from tempfile import TemporaryDirectory
from uuid import uuid4


class FileStore:
    """Adapters provide _file_client() and _file_bucket(); IDs contain no secrets."""

    def store_file(self, filename):
        """Stream a file to a unique object and return its portable string ID.

        Each successful call creates a new artifact, including task retries.
        Administrators must configure retention for abandoned retry artifacts.
        """
        bucket = self._file_bucket()
        key = f"oscar-files/{uuid4().hex}"
        file_id = f"s3://{bucket}/{key}"
        self._file_location(file_id)
        with Path(filename).open("rb") as source:
            self._file_client().upload_fileobj(source, bucket, key)
        return file_id

    def _file_location(self, file_id):
        if not isinstance(file_id, str):
            raise ValueError("File ID must be the string returned by store_file")
        match = re.fullmatch(r"s3://([a-z0-9][a-z0-9.-]{1,61}[a-z0-9])/(oscar-files/[0-9a-f]{32})", file_id)
        if not match or match[1] != self._file_bucket():
            raise ValueError("Invalid file ID or bucket differs from this storage connection")
        return match[1], match[2]

    def retrieve_file(self, file_id, destination):
        """Stream to a new local file; never overwrite an existing destination.

        Failed transfers remove their partial file. The caller owns successful
        files; use open_file for automatic temporary-file cleanup.
        """
        bucket, key = self._file_location(file_id)
        path = Path(destination)
        # Exclusive creation prevents overwriting files or following existing symlinks.
        output = os.fdopen(os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), "wb")
        try:
            with output:
                self._file_client().download_fileobj(bucket, key, output)
        except BaseException:
            path.unlink(missing_ok=True)
            raise
        return str(path)

    @contextmanager
    def open_file(self, file_id):
        """Yield a temporary local Path, removed on normal exit or task failure."""
        with TemporaryDirectory(prefix="oscar-file-") as directory:
            yield Path(self.retrieve_file(file_id, Path(directory) / "artifact"))
