# simple-workQ
A prototype implementation of a very simple work queue based on redis

Planning two endpoints:
- To submit a job (which returns a unique identifier for the job)
- To query with job id. (either returns job state or result if finished)


## Project outline

- `ws` module will have two services, one to submit a summation job; another to query the status of the job.
- `simpleq` module will contain simple task queue system and helpers
- `task` is the worker, which depends on `simpleq` module. 

Planning to use multiprocessing module for worker. Configurations for number of process and anyother settins will be
passed on to `subscribe_to` decorator. Which will be spawning processes accordingly.

Every published message will be put into corresponding queue
Worker will be listening, once item is available in the queue it will be moved to corresponding work queue
eg: queue1 -> w::queue1

Once, a worker successfully process the message, it has to ack back.
Which will remove the message from worker queue. then the result has to be stored in result queue in same transaction.



