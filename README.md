# Stats
<img src="https://img.shields.io/pypi/dm/PRQ?style=for-the-badge" alt="pypi">

# Python parallel task-queue with redis


Based on
* rq
* redis
* gevent

Environment variables
* `PRQ_LOGS` - path to file with logs (512Mb data)




PipeLine
==
Every pipeline part use `.wait()` to get result, if you want, it not block other task, task run in parallel.

Example to test
1. Start worker
```
python prq.py --url redis://localhost:6379
```
(export python path before)
2. (a) Start pipeline (2 task)
``` 
python examples/pipeline/task_pipeline.py
```
2. (b) Start another task in parallel execution
```
python examples/tasks/default_task.py
```
