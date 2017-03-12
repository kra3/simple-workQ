# _*_ coding: utf-8 _*_
import json
import uuid
from .simpleq.jobs import SimpleJobQueue

__author__ = 'Arun KR (@kra3)'

jq = SimpleJobQueue()

@jq.publish_to('sum_2_nums')
def add(num1, num2):
    return {
        'num1': num1,
        'num2': num2,
    }

def query(job_id):
    return jq.get_status(job_id)



if __name__ == '__main__':
    job_id = add(1, 2)
