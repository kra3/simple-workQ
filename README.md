# simple-workQ
A prototype implementation of a very simple work queue based on redis

Planning two endpoints:
- To submit a job (which returns a unique identifier for the job)
- To query with job id. (either returns job state or result if finished)


## Project outline

- `ws` module will have two services, one to submit a summation job; another to query the status of the job.
- `simpleq` module will contain simple task queue system and helpers
- `task` is the worker, which depends on `simpleq` module. 

Every published message will be kept in a hash with associated meta data and it's job id will be put into schedule queue.

Subscriber process will fetch jobs from schedule queue and move into work queue. Meta data (such as timestamp and status) will be updated with each step.

PS: another thread is needed which should go through worker queue and move long running jobs back to schedule queue. We could spawn it in the subscribe decorator itself, or better kept outside as a separate process.

I'd prefer later, then we have to keep track of queues with subscribers and iterate on corresponding work queues only.
