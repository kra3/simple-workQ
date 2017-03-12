# _*_ coding: utf-8 _*_

import json
import uuid
from datetime import datetime

__author__ = 'Arun KR (@kra3)'


class Job(object):
    def __init__(self, content, queue_name):
        self._content = content
        self._id = uuid.uuid4()
        self._orig_queue = queue_name
        self._created_at = datetime.now()
        return self._id

    def build(self):
        res = {
            'header': {
                'id': self._id,
                'orig_queue': self._orig_queue,
                'created_at': self._created_at,
            },
            'payload': self._content,
        }
        return json.dumps(res)

    @property
    def job_id(self):
        return self._id

    @staticmethod
    def load_job(job):
        return json.loads(job)


class Result(object):
    def __init__(self, _id, content):
        self._content = content
        self._id = _id
        self._created_at = datetime.now()

    def build(self):
        res = {
            'header': {
                'id': self._id,
                'created_at': self._created_at,
            },
            'result': self._content,
        }
        return json.dumps(res)


class SimpleJobQueue(object):
    def __init__(self):
        # set up redis connection here
        pass

    def publish_to(self, queue_name):
        def _publish(func):
            def _inner(*args, **kwargs):
                res = func(*args, **kwargs)
                job = Job(res, queue_name)
                # get queue and publish
                # redis.lpush(queue_name, job.build())
                return job.job_id
            return _inner
        return _publish

    def subscribe_to(self, queue_name):
        def _subscribe(func):
            def _inner(*args, **kwargs):
                # get queue, start a worker thread to poll in intervals
                # message = redis.brpoplpush(queue_name, WORK_QUEUE, 0)
                # msg = Job.load_job(message)
                # res = func(msg.payload)
                # redis.hset(msg.id, Result(msg.id, res).build())
            return _inner
        return _subscribe

    def ack(self, job_id):
        # acks and remove a message from work queue
        pass

    def requeue(self):
        # poll for tasks which are pending for too long and move to schedule list
        pass

    def failself, job_id):
        # fails and remove a message from work queue to fail queue
        pass
