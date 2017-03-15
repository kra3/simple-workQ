# _*_ coding: utf-8 _*_

import json
import uuid
from datetime import datetime
from multiprocessing import Process
import redis

__author__ = 'Arun KR (@kra3)'


class SimpleJobQueue(object):
    SCHEDULED, IN_PROGRESS, DONE, FAILED = (
        'SCHEDULED', 'IN_PROGRESS', 'DONE', 'FAILED')
    TIMESTAMP, PAYLOAD, STATUS, QUEUE, RESULT = (
        'timestamp', 'payload', 'status', 'queue', 'result')

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
                            self.TIMESTAMP: self._get_timestamp(),
                            self.QUEUE: queue_name,
                            self.STATUS: self.SCHEDULED,
                            self.PAYLOAD: self.package_result(res)
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
            def _worker():
                while True:
                    # move a scheduled item to work queue
                    job_id = self.redis.brpoplpush(
                        self._get_schedule_queue(queue_name),
                        self._get_work_queue(queue_name), 10)

                    if not job_id:
                        continue
                    else:
                        job_id = job_id.decode("utf-8")

                    # update timestamp
                    self.redis.hmset(self._get_data_set(job_id), {
                        self.TIMESTAMP: self._get_timestamp(),
                        self.STATUS: self.IN_PROGRESS
                    })

                    payload = self.redis.hget(
                        self._get_data_set(job_id), self.PAYLOAD)

                    try:
                        res = func(json.loads(payload))
                    except Exception as e:
                        self.redis.hmset(self._get_data_set(job_id), {
                            self.TIMESTAMP: self._get_timestamp(),
                            self.STATUS: self.FAILED,
                            self.RESULT: self.package_result(str(e))
                        })
                    else:
                        self.redis.hmset(self._get_data_set(job_id), {
                            self.TIMESTAMP: self._get_timestamp(),
                            self.STATUS: self.DONE,
                            self.RESULT: self.package_result(res)
                        })

            #  start a worker thread
            p = Process(target=_worker, args=[])
            p.start()
            p.join()

        return _subscribe

    def get_status(self, job_id):
        key = self._get_data_set(job_id)

        if self.redis.exists(key):
            status, result = self.redis.hmget(key, self.STATUS, self.RESULT)
            res = {'status': status}
            if result is not None:
                res['result'] = result
            return res
        else:
            return {'status': 'error', 'reason': 'job not found'}
