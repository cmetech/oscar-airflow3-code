"""Opt-in example: configure oscar_object_storage before copying into dags/."""
from pathlib import Path
from tempfile import TemporaryDirectory

from airflow.sdk import dag, task
from hooks.object_storage_hook import ObjectStorageHook


@dag(schedule=None, catchup=False, is_paused_upon_creation=True, tags=["example", "object-storage"])
def object_storage_handoff():
    @task
    def produce():
        with TemporaryDirectory() as directory:
            source = Path(directory) / "result.txt"
            source.write_text("Produced by an OSCAR workflow\n")
            return ObjectStorageHook().store_file(source)

    @task
    def consume(file_id):
        # Works on any worker; only the ID travels through XCom.
        with ObjectStorageHook().open_file(file_id) as local_file:
            return {"bytes": local_file.stat().st_size}

    consume(produce())


object_storage_handoff()
