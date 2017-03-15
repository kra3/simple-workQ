# _*_ coding: utf-8 _*_
from simpleq.jobs import SimpleJobQueue

__author__ = 'Arun KR (@kra3)'

jq = SimpleJobQueue()


@jq.subscribe_to('sum_2_nums')
def sum_numbers(data):
    return data['num1'] + data['num2']
