from rq import Queue
from redis import Redis
from scripts.job import Job

conn = Redis.from_url("redis://localhost:6379")
queue_high = Queue(name="high", default_timeout=-1, connection=conn, job_class=Job)
