"""Opt-in example: provision connection/bucket before copying into dags/."""
from pathlib import Path
from tempfile import TemporaryDirectory
from urllib.parse import quote

from airflow.sdk import dag, task, get_current_context
from hooks.object_storage_hook import ObjectStorageHook


@dag(schedule=None, catchup=False, is_paused_upon_creation=True, tags=["example", "object-storage"])
def object_storage_handoff():
    @task
    def produce():
        context = get_current_context()
        key = f"handoff/{quote(context['run_id'], safe='')}/result.txt"
        with TemporaryDirectory() as directory:
            source = Path(directory) / "result.txt"
            source.write_text("Produced by an OSCAR workflow\n")
            return ObjectStorageHook().upload_file(source.as_posix(), "workflow-artifacts", key, replace=True)

    @task
    def consume(reference):
        with TemporaryDirectory() as directory:
            path = ObjectStorageHook().download_file(
                key=reference["key"], bucket_name=reference["bucket"], local_path=directory,
            )
            # Return a small result, not the artifact content.
            return {"bytes": Path(path).stat().st_size}

    consume(produce())


object_storage_handoff()
