# simple-workQ
A prototype implementation of a very simple work queue using redis and scala.

Planning two endpoints:
- To submit a job (which returns a unique identifier for the job)
- To query with job id. (either returns job state or result if finished)
