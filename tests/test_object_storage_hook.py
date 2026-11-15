"""Run with unittest inside the workflow image (real installed Amazon provider)."""
import json
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

from airflow.sdk import Connection
from botocore.exceptions import ClientError
from botocore.stub import Stubber

sys.path.insert(0, str(Path(__file__).parents[1] / "plugins"))
from hooks.object_storage_hook import ObjectStorageHook


class ObjectStorageHookTests(unittest.TestCase):
    def connection(self, **extra):
        return Connection(conn_id="test_storage", conn_type="aws", login="scoped-key",
                          password="test-secret", extra=json.dumps({"endpoint_url": "https://rustfs:9000", **extra}))

    def hook(self, connection=None):
        lookup = patch.object(ObjectStorageHook, "get_connection", return_value=connection or self.connection())
        self.addCleanup(lookup.stop)
        lookup.start()
        return ObjectStorageHook("test_storage")

    def test_requires_explicit_connection_and_no_ambient_fallback(self):
        with self.assertRaises(ValueError):
            ObjectStorageHook("")
        with patch.object(ObjectStorageHook, "get_connection", side_effect=LookupError("missing")):
            with self.assertRaises(LookupError):
                ObjectStorageHook().get_conn()

    def test_tls_endpoint_and_signing_on_real_client(self):
        hook = self.hook()
        client = hook.get_conn()
        self.assertEqual(client.meta.endpoint_url, "https://rustfs:9000")
        self.assertEqual(client.meta.config.signature_version, "s3v4")
        self.assertEqual(client.meta.config.s3["addressing_style"], "path")
        self.assertEqual(hook.conn_config.verify, "/opt/airflow/ssl/ca-cert.pem")

    def test_rejects_unsafe_connection_options(self):
        cases = [{"endpoint_url": value} for value in ["", "http://rustfs:9000", "https://user:pass@rustfs:9000", "https://rustfs/path", "https://rustfs?key=secret"]]
        cases += [{"verify": False}, {"verify": ""}, {"verify": 0}, {"role_arn": "arn:test"}, {"profile_name": "default"},
                  {"service_config": {"s3": {"endpoint_url": "http://rustfs:9000"}}}]
        for extra in cases:
            with self.subTest(extra=extra), self.assertRaises(ValueError):
                self.hook(self.connection(**extra)).get_conn()

    def test_rejects_missing_keys(self):
        connection = self.connection()
        connection.password = None
        with self.assertRaises(ValueError):
            self.hook(connection).get_conn()

    def test_upload_returns_only_reference_and_overwrite_is_explicit(self):
        hook = self.hook()
        with patch.object(hook, "load_file") as upload:
            self.assertEqual(hook.upload_file("/tmp/data", "workflow", "run/data"), {"bucket": "workflow", "key": "run/data"})
            upload.assert_called_once_with(filename="/tmp/data", bucket_name="workflow", key="run/data", replace=False)
        with patch.object(hook, "load_file", side_effect=RuntimeError("upload failed")):
            with self.assertRaises(RuntimeError):
                hook.upload_file("/tmp/data", "workflow", "run/data")

    def test_provider_write_and_permission_failure_propagate(self):
        hook = self.hook()
        with Stubber(hook.get_conn()) as stub:
            stub.add_response("put_object", {}, {"Bucket": "workflow", "Key": "data", "Body": b"hello"})
            hook.get_conn().put_object(Bucket="workflow", Key="data", Body=b"hello")
            stub.add_client_error("get_object", service_error_code="AccessDenied", http_status_code=403)
            with self.assertRaises(ClientError):
                hook.get_conn().get_object(Bucket="denied", Key="data")
            stub.assert_no_pending_responses()


if __name__ == "__main__":
    unittest.main()
