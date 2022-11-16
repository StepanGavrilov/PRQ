from scripts.job import Job
from examples.connection import queue_high, conn  # type: ignore # noqa F401
from examples.tasks import entrypoint, greeting  # type: ignore

from rq.job import Retry

job1: Job = queue_high.enqueue(
    f=greeting,
    description="task1",
    retry=Retry(2),
)
job2: Job = queue_high.enqueue(
    f=entrypoint,
    description="task2",
    retry=Retry(2),
    depends_on=job1.id,
    args=(job1.id,),
)
print(job1.id)
print(job2.id)
