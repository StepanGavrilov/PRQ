#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import absolute_import, division, print_function, unicode_literals

import argparse
import os
import sys

from redis.exceptions import ConnectionError
from rq import Queue
from rq.contrib.legacy import cleanup_ghosts
from rq.logutils import setup_loghandlers
from rq.utils import import_attribute

import prq
from scripts import (
    add_standard_arguments,
    read_config_file,
    setup_default_arguments,
    setup_redis,
    Job,
)


def parse_args():
    parser = argparse.ArgumentParser(description="Starts an RQ worker.")
    add_standard_arguments(parser)

    parser.add_argument(
        "--burst",
        "-b",
        action="store_true",
        default=False,
        help="Run in burst mode (quit after all work is done)",
    )  # noqa
    parser.add_argument("--name", "-n", default=None, help="Specify a different name")
    parser.add_argument(
        "--worker-class",
        "-w",
        action="store",
        default="rq.Worker",
        help="RQ Worker class to use",
    )
    parser.add_argument("--path", "-P", default=".", help="Specify the import path.")
    parser.add_argument(
        "--results-ttl", default=None, help="Default results timeout to be used"
    )
    parser.add_argument(
        "--worker-ttl", default=None, help="Default worker timeout to be used"
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true", default=False, help="Show more output"
    )
    parser.add_argument(
        "--quiet", "-q", action="store_true", default=False, help="Show less output"
    )
    parser.add_argument(
        "--pid",
        action="store",
        default=None,
        help="Write the process ID number to a file at the specified path",
    )

    return parser.parse_args()


def setup_loghandlers_from_args(args):
    if args.verbose and args.quiet:
        raise RuntimeError("Flags --verbose and --quiet are mutually exclusive.")
    if args.verbose:
        level = "DEBUG"
    elif args.quiet:
        level = "WARNING"
    else:
        level = "INFO"
    setup_loghandlers(level)


def main():
    args: argparse.Namespace = parse_args()
    if args.path:
        sys.path = args.path.split(":") + sys.path

    settings = {}
    if args.config:
        settings = read_config_file(args.config)

    setup_default_arguments(args, settings)

    # Worker specific default arguments

    if args.pid:
        with open(os.path.expanduser(args.pid), "w") as fp:
            fp.write(str(os.getpid()))

    setup_loghandlers_from_args(args)
    setup_redis(args)

    cleanup_ghosts()
    worker_class = import_attribute(args.worker_class)
    queue_names = ["high", "medium", "low"]
    try:
        queues = [Queue(name=q_name, job_class=Job) for q_name in queue_names]
        w: prq.GeventWorker = worker_class(  # type: ignore
            queues,
            name=args.name,
            default_worker_ttl=-1,
            default_result_ttl=args.results_ttl,
            job_class=Job,
            redis_url=args.url,
        )

        w.work(burst=args.burst)
    except ConnectionError as e:
        print(e)
        sys.exit(1)
