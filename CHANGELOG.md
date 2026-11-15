# Changelog

## 2026-11-15

- Add ObjectStorageHook for direct RustFS S3 file access using a scoped Airflow connection, required TLS, SigV4 and path-style addressing, without ambient AWS credential fallback.
- Add reference-returning file upload and inherit provider download/text/bytes/list operations. Supply an opt-in two-task example and Vault setup/rotation/retention guidance.
- Verify six provider-backed tests, credential-free DAG parsing and a live 2 MiB scoped file handoff; no permanent connection or scheduled DAG deployed.
