# _*_ coding: utf-8 _*_

import json
import uuid
from datetime import datetime
import redis

__author__ = 'Arun KR (@kra3)'


class SimpleJobQueue(object):

    def __init__(self, host='localhost', port=6379):
        self._redis = redis.StrictRedis(host=host, port=port)

    @property
    def redis(self):
        return self._redis

    def _get_schedule_queue(self, queue_name):
        return 'SCHED::{}'.format(queue_name)

    def _get_work_queue(self, queue_name):
        return 'WORK::{}'.format(queue_name)

    def _get_data_set(self, job_id):
        return "DATA::{}".format(job_id)

    def _get_result_set(self, job_id):
        return "RESULT::{}".format(job_id)

    def _build_unique_id(self):
        return uuid.uuid4().hex

    def _get_timestamp(self):
        return datetime.now().timestamp()

    def package_result(self, res):
        return json.dumps(res)

    def publish_to(self, queue_name):
        def _publish(func):
            def _inner(*args, **kwargs):
                res = func(*args, **kwargs)
                job_id = self._build_unique_id()

                with self.redis.pipeline() as pipe:
                    try:
                        pipe.hmset(self._get_data_set(job_id), {
                            'timestamp': self._get_timestamp(),
                            'payload': self.package_result(res)
                        })
                        pipe.lpush(
                            self._get_schedule_queue(queue_name), job_id)
                        pipe.execute()
                    except Exception as e:
                        print(e)

                return job_id
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
                pass
            return _inner
        return _subscribe

    def ack(self, job_id):
        # acks and remove a message from work queue
        pass

    def requeue(self):
        # poll for tasks which are pending for too long and move to schedule
        # list
        pass

    def fail(self, job_id):
        # fails and remove a message from work queue to fail queue
        pass
