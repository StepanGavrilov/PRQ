from rq.job import Retry
from examples.tasks import foo  # type: ignore
from examples.connection import queue_high, conn  # type: ignore # noqa F401

queue_high.enqueue(f=foo, retry=Retry(2))
