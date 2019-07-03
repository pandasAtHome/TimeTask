# -*- coding:utf-8 -*-
import time
import task
from mytools import tools


class Test(object):
    @staticmethod
    def print_params():
        print(task.params)

    @staticmethod
    def task1():
        msg = '''create time: {}
        msg: this is task1'''.format(task._create_date)
        with open(tools.ROOT_PATH + '/log/test.txt', 'w', encoding = 'utf-8') as fpw:
            fpw.write(msg)
        time.sleep(3)
        # 结束当前进程任务  ->  开启新进程，执行下一任务
        tools.request('/Test/task2', task.params)

    @staticmethod
    def task2():
        msg = '''create time: {}
        msg: this is task2'''.format(task._create_date)
        with open(tools.ROOT_PATH + '/log/test.txt', 'a', encoding = 'utf-8') as fpw:
            fpw.write(msg)
