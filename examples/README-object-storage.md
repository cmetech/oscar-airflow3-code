# Object storage between workflow tasks

`hooks.object_storage_hook.ObjectStorageHook` uses the installed Amazon S3 provider directly against RustFS. File bytes do not pass through OSCAR Middleware. No new middleware route, provider dependency or database migration is needed. OSCAR's existing middleware hooks remain appropriate for OSCAR business records; object storage uses S3's streaming/multipart transfers and bucket policies.

## Administrator setup

1. Create an application bucket and scoped read/write credentials with the existing utility, from the backend directory:
   ```sh
   ./oscar exec python rustfs_manager.py create-bucket workflow-artifacts
   ./oscar exec python rustfs_manager.py app-create airflow-artifacts workflow-artifacts write
   ```
   Provision this explicitly, not during every task or startup. Fresh installs remain empty. Do not give tasks RustFS root keys or the credential-manager AppRole. Use separate buckets/connections for separate trust boundaries.
2. In OSCAR's Connections management, create `oscar_object_storage`, type **Object Storage (S3-compatible)**, stored in Vault. Set Access key / Secret key to the scoped keys from `kv/oscar/objectstorage/apps/airflow-artifacts/credentials`. Never put credentials in a DAG, command argument, log, or XCom.
3. In the dedicated storage settings, set the HTTPS endpoint, default bucket and region. Leave the optional CA certificate path blank for the OSCAR CA. These fields save the following compatible connection extras:
   ```json
   {
     "endpoint_url": "https://rustfs:9000",
     "region_name": "us-east-1",
     "bucket_name": "workflow-artifacts"
   }
   ```
   The UI stores the underlying connection type as `aws`, which both helpers already support. Connection IDs are stored in lowercase; use that ID in `conn_id`. Editing a connection retains its hidden secret unless you enter a replacement. Clearing the custom CA path restores each worker’s default OSCAR CA. The wizard provisions a connection only; it does not create buckets or grant bucket permissions.

   OSCAR Connections writes the standard `conn_uri` record at `kv/airflow/connections/oscar_object_storage`; this format is readable by both Airflow and TaskManager's existing connection lookup. A manually created Vault record containing separate login/password fields works with Airflow but is **not** the shared format expected by that middleware lookup. The utility application record is not automatically published as a connection.

   Leave `verify` unset for a shared internal connection: each adapter uses its worker's OSCAR CA path (`/opt/airflow/ssl/ca-cert.pem` for Airflow; `CA_CERTFILE`, normally `/home/splunk/ssl/ca-cert.pem`, for TaskManager). For custom trust, a `verify` certificate path must exist in every consumer container. Credentials and S3 traffic require verified HTTPS. Remote workers need network/DNS access to a reachable HTTPS gateway and its trusted CA.
4. Deploy the code using the existing `./oscar workflow sync --v3` and workflow release process. The hook lives in the Airflow 3 source repository; modifying that checkout alone does not deploy it. No service restart or metadata migration was performed as part of this change.
5. Optionally copy `examples/object_storage_handoff.py` into the managed DAG source, sync it, and trigger it manually after provisioning. It has no schedule and starts paused. `examples/` is outside the DAG scan path.

Airflow's generic AWS **Test Connection** uses STS GetCallerIdentity, which may fail against an S3-compatible service even when file operations work. Validate by uploading/downloading a disposable object in the assigned bucket instead.

## Simple file-ID interface (Airflow and tasks)

```python
# Airflow, inside a task:
from hooks.object_storage_hook import ObjectStorageHook
storage = ObjectStorageHook()
file_id = storage.store_file("/tmp/report.json")
# Return file_id; TaskFlow passes this small string through XCom.

# In another task, even on a different worker:
with ObjectStorageHook().open_file(file_id) as path:
    process_report(path)  # your application function
# Temporary local copy is removed, including when processing raises an error.
```

For a file you want to retain locally:
`storage.retrieve_file(file_id, "/tmp/my-report.json")`. The destination must not
exist; its parent directory must already exist. Failed downloads remove partial
files. The helper never overwrites a preexisting local file.

OSCAR Python/Fabric tasks expose the same methods:

```python
from utils.helper import ObjectStorageManager
from helper.output import format_output

file_id = ObjectStorageManager().store_file("/tmp/report.json")
format_output(data={"file_id": file_id})
# Existing script output envelope places this at output.file_id.
```

Pass `output.file_id` into the downstream task's `file_id` argument using your
existing orchestration/result mapping. The helper does not automatically connect
unrelated tasks or rewrite their arguments. Consumer code:

```python
from utils.helper import ObjectStorageManager

def consume(file_id):
    with ObjectStorageManager().open_file(file_id) as path:
        return {"bytes": path.stat().st_size}
```

Fabric/server helper modules also re-export ObjectStorageManager. The lightweight
direct import is `from tm_credentials.object_storage import ObjectStorageManager`.
Both APIs accept `conn_id=` and an optional `bucket_name=` override, subject to
server-side permissions. Rebuild TaskManager to package its new helper module;
Airflow uses the normal managed code-sync/deployment path.

The ID is a string of the form `s3://<bucket>/oscar-files/<random UUID>`. Authors
should pass it unchanged. It contains no hostname, credentials or signed URL and
does not grant access: the consumer still needs a scoped connection authorized
for that bucket, pointing to the same storage installation. Each adapter rejects
IDs outside its configured bucket. Separate authorized connections can share an
ID, including read-only consumers. The older `{bucket, key}` return value from
upload_file remains unchanged and uses the lower-level download_file API.

Each successful store_file call creates a new ID; retries can leave unreferenced
artifacts. Configure lifecycle retention for the `oscar-files/` prefix long enough
to cover downstream retries and delayed consumers. open_file cleans only the local
copy. Objects persist until lifecycle/admin cleanup; no automatic remote deletion,
new SQL file registry, or permissions grant is introduced. Unique IDs avoid normal
writer collisions, but are not Object Lock or a guarantee against an authorized
administrator changing/deleting an object. A deleted/expired or inaccessible ID
raises the storage error so task retry/failure policy remains in control.

## Lower-level workflow API

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

Upload defaults to `replace=False`. Choose unique keys scoped by workflow/run/task; explicitly use `replace=True` for an idempotent retry of the same artifact. The provider's no-overwrite check is not atomic, so do not use it to coordinate concurrent writers. Retries should use stable keys; concurrent runs need different keys. References point to the current key, not an immutable version. This advanced API remains available; the simple example uses store_file instead, which assigns a unique ID per successful call.

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

### File-ID qualification — 2026-11-16

Ten TaskManager unit tests (plus 17 parameterized subtests) and seven Airflow
provider-backed tests passed. The shared file-reference implementation is identical
in both distributions. Live verification used independent TaskManager and Airflow
containers: TaskManager resolved a temporary Vault-backed connection through the
real Middleware API and stored a 2 MiB binary file; Airflow consumed that ID and
stored another ID; TaskManager consumed the latter. Byte comparisons and temporary
file cleanup passed in both directions. Airflow used a process-local connection
environment for this standalone test, not a scheduled TaskFlow execution. Test
connection/object/bucket/policy cleanup completed and the scoped key was revoked.

The Object Storage wizard requires the matching AdminUI and Middleware update. Complete AWS storage settings (endpoint_url, bucket_name and region_name together) replace stored extras on update, allowing removed optional settings to clear; partial updates retain the existing merge behavior.
