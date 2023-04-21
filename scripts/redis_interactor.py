from rq.registry import (
    StartedJobRegistry,
    FinishedJobRegistry,
    FailedJobRegistry,
    DeferredJobRegistry,
    ScheduledJobRegistry,
    CanceledJobRegistry,
)


import redis

from scripts import Job
from itertools import chain


def flush_job_pids(url) -> int:
    """
    clean old job data: pid
    when restart / start
    so as not to accidentally kill,
    actual jobs
    """

    registries = (
        StartedJobRegistry,
        FinishedJobRegistry,
        FailedJobRegistry,
        DeferredJobRegistry,
        ScheduledJobRegistry,
        CanceledJobRegistry,
    )

    redis_conn = redis.Redis.from_url(url=url)
    flushed_jobs = [
        redis_conn.hdel(f"rq:job:{j}", "pid")
        for j in list(
            chain.from_iterable(
                list(
                    map(
                        lambda j: j(
                            name="high", connection=redis_conn, job_class=Job
                        ).get_job_ids(),
                        registries,
                    )
                )
            )
        )
    ]

    return len(flushed_jobs)
