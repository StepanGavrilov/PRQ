# -*- coding: utf-8 -*-

from __future__ import (
    absolute_import, division,
    print_function, unicode_literals
)

import signal

import gevent
import gevent.pool
# import gtools.tree  # type: ignore
# import gtools
from gevent import monkey, get_hub
from gevent.hub import LoopExit
from rq import Worker
from rq.exceptions import DequeueTimeout
from rq.logutils import setup_loghandlers
from rq.timeouts import BaseDeathPenalty, JobTimeoutException
from rq.version import VERSION
from rq.worker import StopRequested, green, blue

monkey.patch_all()

try:  # for rq >= 0.5.0
    from rq.job import JobStatus, Job  # noqa F401
except ImportError:  # for rq <= 0.4.6
    from rq.job import Status as JobStatus  # noqa F401


class GeventDeathPenalty(BaseDeathPenalty):
    def __init__(self, timeout, exception=JobTimeoutException, **kwargs):
        super().__init__(timeout, exception, **kwargs)
        self.gevent_timeout = None

    def setup_death_penalty(self):
        exception = JobTimeoutException('Gevent Job exceeded maximum timeout value (%d seconds).' % self._timeout)
        self.gevent_timeout = gevent.Timeout(
            seconds=None,
            exception=exception
        )
        self.gevent_timeout.start()

    def cancel_death_penalty(self):
        self.gevent_timeout.cancel()


class GeventWorker(Worker):
    death_penalty_class = GeventDeathPenalty

    def __init__(self, *args, **kwargs):
        self.gevent_pool = gevent.pool.Pool(kwargs.get("pool_size", 20))
        super(GeventWorker, self).__init__(*args, **kwargs)

    def register_birth(self):
        self.log.info(
            msg=f"Info on Birth\n"
                f"=======\n"
                f"pool size: {self.gevent_pool.size}\n"
                f"timeout: {self.default_worker_ttl}\n"
                f"pid: {self.pid}\n"
                f"state: {self.state}\n"
                f"queues: {self.queues}\n"
                f"job class: {self.job_class}\n"
                f"=======\n"
        )
        super(GeventWorker, self).register_birth()
        self.connection.hset(self.key, 'pool_size', self.gevent_pool.size)

    def heartbeat(self, timeout=0, pipeline=None):
        connection = pipeline if pipeline is not None else self.connection
        super(GeventWorker, self).heartbeat(timeout)
        connection.hset(self.key, 'curr_pool_len', len(self.gevent_pool))

    def _install_signal_handlers(self):
        def request_force_stop():
            self.log.warning('Cold shut down.')
            self.gevent_pool.kill()
            raise SystemExit()

        def request_stop():
            print("STOP REQUESTED!")

            gevent.signal_handler(signal.SIGINT, request_force_stop)
            gevent.signal_handler(signal.SIGTERM, request_force_stop)

            self.log.warning('Warm shut down requested.')
            self.log.warning('Stopping after all greenlets are finished. '
                             'Press Ctrl+C again for a cold shutdown.')

            self._stopped = True
            self.gevent_pool.join()

            # import gc
            # from greenlet import greenlet
            # print('=====>', [obj for obj in gc.get_objects() if isinstance(obj, gevent.Greenlet)])
            # for i in gc.get_objects():
            #     if isinstance(i, gevent.Greenlet):
            #         print(type(i))
            # try:
            #     gevent.killall([obj for obj in gc.get_objects() if isinstance(obj, gevent.Greenlet)])
            # except greenlet.GreenletExit:
            #     pass
            # print("=====>")
            # print([obj for obj in gc.get_objects() if isinstance(obj, gevent.Greenlet)])

        gevent.signal_handler(signal.SIGINT, request_stop)
        gevent.signal_handler(signal.SIGTERM, request_stop)

    def set_current_job_id(self, job_id, pipeline=None):
        pass

    def work(self, burst=False):
        """Starts the work loop.

        Pops and performs all jobs on the current list of queues.  When all
        queues are empty, block and wait for new jobs to arrive on any of the
        queues, unless `burst` mode is enabled.

        The return value indicates whether any jobs were processed.
        """

        setup_loghandlers()
        self._install_signal_handlers()
        self.did_perform_work = False
        self.register_birth()
        self.log.info('RQ worker started, version %s' % VERSION)

        self.set_state('starting')
        try:
            while True:
                if self._stop_requested:
                    self.log.info('Stopping on request.')
                    break

                timeout = None \
                    if burst else max(1, self.default_worker_ttl - 60)

                try:
                    result = self.dequeue_job_and_maintain_ttl(timeout)

                    # TODO add tree in future
                    # import gtools.tree
                    # greenlet_tree = gtools.tree.Tree(all=True)
                    # print(greenlet_tree)
                    # print(greenlet_tree.to_dict())

                    if result is None and burst:
                        try:
                            get_hub().switch()
                        except LoopExit:
                            pass
                        result = self.dequeue_job_and_maintain_ttl(timeout)

                    if result is None:
                        break
                except StopRequested:
                    break

                job, queue = result
                self.execute_job(job, queue)

        finally:
            if not self.is_horse:
                self.register_death()
        return self.did_perform_work

    def execute_job(self, job: Job, queue):
        self.log.info(
            msg=f"Start executing a job - "
                f"id: {job.id} "
                f"ttl: {job.ttl} "
                f"result ttl: {job.result_ttl}"
        )
        self.gevent_pool.spawn(self.perform_job, job, queue)

    def dequeue_job_and_maintain_ttl(self, timeout):
        if self._stop_requested:
            raise StopRequested()

        result = None

        while True:
            if self._stop_requested:
                raise StopRequested()
            self.heartbeat()
            while self.gevent_pool.full():
                gevent.sleep(0.1)
                if self._stop_requested:
                    raise StopRequested()
            try:
                result = self.queue_class.dequeue_any(
                    self.queues,
                    timeout,
                    connection=self.connection
                )
                if result is not None:
                    # fetch
                    job, queue = result
                    self.log.info('%s: %s (%s)' % (green(queue.name),
                                                   blue(job.description), job.id))
                break
            except DequeueTimeout:
                pass

        self.heartbeat()
        return result


def worker():
    """
    Start worker
    """

    import sys
    from scripts.rqworker import main as rq_main

    if '-w' in sys.argv or '--worker-class' in sys.argv:
        print("You cannot specify worker class when using this script,"
              "use the official rqworker instead")
        sys.exit(1)

    sys.argv.extend(['-w', 'prq.GeventWorker'])
    rq_main()


if __name__ == '__main__':
    worker()
