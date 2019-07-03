# -*- coding: utf-8 -*-
import re
import os

# 引入自定义模块
from mytools import glob
from mytools import tools


VERSION = 1.0

# 开始执行时间
_create_date = glob.get_value('create_time')

# 请求参数解析：字典类型
params = glob.get_value('params')
params = tools.url_decode(params) if params else {}


def check_task():
    """检测是否存在重复任务
    - 由于次函数内使用的检测命令是针对Linux的
    - 所以，在 windows 操作系统中，检测重复会失效
    """
    pid = os.getpid()  # 本次任务进程id
    _class, _func = glob.get_value('path').split('/')  # 获取执行命令行时的类名、方法名
    out = tools.request_cmd('ps ax -o pid,command | grep {}/{}'.format(_class, _func), True)[0].split('\n')
    # 正则文档：https://blog.csdn.net/qq_37968529/article/details/81198654
    pattern = 'python.+\/index\.py.+{}\/{}'.format(_class, _func)
    pattern = re.compile(pattern, re.I)  # 预编译，不区分大小写
    for o in out:
        # 排除本次执行
        o = o.strip(str(pid)).strip()
        if not o or pattern.match(o):
            continue
        # 检测旧任务
        result = pattern.search(o)
        if result and 'again' not in params:
            msg = '''This task is in progress. 
            If your still need to execute, add request parameter "again=true"
            '''
            raise ValueError(msg)


check_task()
