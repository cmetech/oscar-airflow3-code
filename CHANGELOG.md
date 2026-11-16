## Object Storage connection setup — 2026-11-16

- Document the dedicated OSCAR Object Storage connection wizard and connection-ID/CA behavior.

## Workflow/task file-ID helpers — 2026-11-16

- Add store_file, retrieve_file and open_file interfaces for shared file references across Airflow and TaskManager workers, using scoped connections and direct verified-TLS RustFS transfers.
- Preserve the existing Airflow bucket/key API; add task helper exports, a simplified TaskFlow example, validation/cleanup tests, and shared connection/retention guidance. Requires managed Airflow code sync and a TaskManager image rebuild; no schema migration or automatic bucket creation.

# Changelog

## 2026-11-15

- Add ObjectStorageHook for direct RustFS S3 file access using a scoped Airflow connection, required TLS, SigV4 and path-style addressing, without ambient AWS credential fallback.
- Add reference-returning file upload and inherit provider download/text/bytes/list operations. Supply an opt-in two-task example and Vault setup/rotation/retention guidance.
- Verify six provider-backed tests, credential-free DAG parsing and a live 2 MiB scoped file handoff; no permanent connection or scheduled DAG deployed.
