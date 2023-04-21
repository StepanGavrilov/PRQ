import subprocess

from scripts.job import Job
from runnner import Runner


def greeting():
    subprocess.Popen(
        "python examples/work_with_files/hello_world.py", shell=True
    ).wait()
    return {"detail": {"response": {"data": [2, 4, 6]}}}


def entrypoint(job_id: str):
    prev_job = Job.fetch(job_id)
    print(f"get prev result: {job_id} result: {prev_job.result}")
    subprocess.Popen(
        "python examples/work_with_files/hello_world2.py", shell=True
    ).wait()


def foo():
    Runner(
        entrypoint_path="python examples/work_with_files/hello_world3.py",
        wait_result=True,
    ).run()
