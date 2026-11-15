# Object storage between workflow tasks

`hooks.object_storage_hook.ObjectStorageHook` uses the installed Amazon S3 provider directly against RustFS. File bytes do not pass through OSCAR Middleware. No new middleware route, provider dependency or database migration is needed. OSCAR's existing middleware hooks remain appropriate for OSCAR business records; object storage uses S3's streaming/multipart transfers and bucket policies.

## Administrator setup

1. Create an application bucket and scoped read/write credentials with the existing utility, from the backend directory:
   ```sh
   ./oscar exec python rustfs_manager.py create-bucket workflow-artifacts
   ./oscar exec python rustfs_manager.py app-create airflow-artifacts workflow-artifacts write
   ```
   Provision this explicitly, not during every task or startup. Fresh installs remain empty. Do not give tasks RustFS root keys or the credential-manager AppRole. Use separate buckets/connections for separate trust boundaries.
2. Create an Airflow connection named `oscar_object_storage`, type **Amazon Web Services (`aws`)**. Its login is the scoped access key and its password is the scoped secret key. Retrieve these securely from the utility-managed Vault record `kv/oscar/objectstorage/apps/airflow-artifacts/credentials`; never paste them into a DAG, command argument, log, or XCom.
3. For the configured OSCAR Vault secrets backend, publish the Airflow connection at **`kv/airflow/connections/oscar_object_storage`**, using fields `conn_type=aws`, `login`, `password`, and `extra` (the JSON string below). A secure Airflow connection managed through its UI is also supported, but an existing Vault connection takes precedence. The utility application record is not itself an Airflow connection and is not discovered automatically.
   ```json
   {
     "endpoint_url": "https://rustfs:9000",
     "region_name": "us-east-1",
     "verify": "/opt/airflow/ssl/ca-cert.pem"
   }
   ```
   Workers already have the OSCAR CA mounted at this path. For external workers, use the reachable HTTPS gateway origin (for example `https://oscar.example:9444`) and mount its trusted CA; `verify: true` selects system trust. Disabling TLS verification, missing credentials, ambient AWS profiles and AWS role assumptions are rejected. The hook enforces SigV4 and path-style addressing.
4. Deploy the code using the existing `./oscar workflow sync --v3` and workflow release process. The hook lives in the Airflow 3 source repository; modifying that checkout alone does not deploy it. No service restart or metadata migration was performed as part of this change.
5. Optionally copy `examples/object_storage_handoff.py` into the managed DAG source, sync it, and trigger it manually after provisioning. It has no schedule and starts paused. `examples/` is outside the DAG scan path.

Airflow's generic AWS **Test Connection** uses STS GetCallerIdentity, which may fail against an S3-compatible service even when file operations work. Validate by uploading/downloading a disposable object in the assigned bucket instead.

## Workflow authors

```python
from hooks.object_storage_hook import ObjectStorageHook

# Inside a task, after producing a file:
reference = ObjectStorageHook().upload_file(
    "/tmp/report.json", "workflow-artifacts", "my-workflow/unique-run/report.json"
)
# Return reference via XCom: {"bucket": "workflow-artifacts", "key": "..."}.

# In a downstream task (possibly on a different worker):
local_file = ObjectStorageHook().download_file(
    bucket_name=reference["bucket"], key=reference["key"], local_path="/tmp"
)
```

Use task-local temporary directories and clean them after consumption. Never pass a producer's local path to another worker. Inherited `load_bytes`, `load_string`, `read_key`, `check_for_key` and `list_keys` are available too. `read_key` loads text into memory: use `download_file` for large/binary artifacts. Return only references or small summaries from tasks; file content does not belong in XCom.

Upload defaults to `replace=False`. Choose unique keys scoped by workflow/run/task; explicitly use `replace=True` for an idempotent retry of the same artifact. The provider's no-overwrite check is not atomic, so do not use it to coordinate concurrent writers. Retries should use stable keys; concurrent runs need different keys. References point to the current key, not an immutable version. The example encodes the run ID and intentionally overwrites on retry.

Task failure does not remove previously uploaded objects. Define retention using administrator-managed lifecycle policies or explicit cleanup tasks with separately authorized deletion. A `write` utility policy allows read/write, not deletion. The hook does not create buckets, grant access, or enable server-side encryption automatically; encryption must be provisioned on the service before requiring it.

## Credential rotation

Run `app-rotate`, securely update the Airflow connection's login/password to the new Vault application keys, and verify new task instances before `app-retire --confirm-clients-updated`. Existing hook clients retain their credential snapshot; allow active tasks to finish before retiring the old key. Construct hooks inside each task invocation. This implementation does not automatically mirror utility credentials into Airflow's connection namespace. `app-revoke` invalidates the managed keys even if an old connection record remains.

An OSCAR administrator's Console session is separate from workflow authorization. Airflow runs as the connection's application identity, with its RustFS bucket policy; it does not inherit the user who clicked Run. Users allowed to author arbitrary DAG code share the worker's trust boundary—an Airflow connection ID alone is not per-DAG isolation.

## Verification

`tests/test_object_storage_hook.py` uses Python unittest and the real Airflow/Amazon provider in the workflow image; no new test dependency is required. It checks TLS and endpoint validation, missing credentials, no ambient fallback, provider configuration, reference-only results and error propagation. The example can be imported without fetching connections because hooks are constructed only inside tasks.

References: [Airflow S3 hook](https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/_api/airflow/providers/amazon/aws/hooks/s3/index.html), [AWS-compatible connections](https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/connections/aws.html), [Vault connection backend](https://airflow.apache.org/docs/apache-airflow-providers-hashicorp/stable/secrets-backends/hashicorp-vault.html).

### Local qualification — 2026-11-15

Verified with the running OSCAR workflow 3.3.2 image and RustFS 1.0.5:

- Six unittest cases passed against the installed Amazon provider, including the unsafe-configuration matrix.
- The example DAG parsed with connection lookup explicitly forbidden during parsing.
- A live 2 MiB binary file uploaded with a disposable scoped utility-managed key and downloaded byte-for-byte through a separate hook instance using the returned bucket/key reference. TLS used the mounted OSCAR CA. Deletion with the read/write key returned 403.
- Test keys were revoked; the object, bucket and IAM policy were removed. The revoked application audit record remains in Vault.

The live test supplied a Connection object from the scoped Vault record in a temporary test harness; it did not publish a permanent Airflow connection or execute a scheduled DAG through the task supervisor. A full TaskFlow/XCom run and credential-rotation handoff remain deployment acceptance checks. Temporary test code was copied only to the worker's `/tmp`, not its managed plugin directory. The installed provider emitted its existing ProvidersManager deprecation warning; no dependency upgrade was made.
