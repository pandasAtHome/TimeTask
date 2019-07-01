#!/usr/bin/python
# -*- coding: utf-8 -*-
# 引入内置模块
import sys
import copy
import traceback

# 引入自定义模块
from mytools import tools
from mytools import glob


def write_log(_msg, _file_type = 'cli', _params = None, _remark = None):
    """
    记录日志
    :param dict or str _msg:
    :param str _params:
    :param str _file_type:
    :param str _remark:
    :return:
    """
    content = {
        'file_type': _file_type,
        'message': _msg,
    }
    if _remark:
        content['remark'] = _remark
    tools.logs('/' + _file_type + '/error.log', content, 'error', _type = 3)


create_time = tools.get_date('%Y-%m-%d %H:%M:%S')
glob.init()  # 初始化全局变量
glob.set_value('create_time', create_time)  # 记录创建时间

# 获取命令行参数：本文件路径, 执行方法路径[, 请求参数[, 其他]]
params = copy.deepcopy(sys.argv)
# 去掉第一个参数：本文件路径
params.pop(0)

# 1、参数验证
if not params:
    exit(write_log('Miss param', _params = sys.argv))

# 执行方法路径：/xxx/xxx
# 2、字符串切割，获取类名、方法名
_path = params.pop(0).strip('/').split('/')
if len(_path) != 2:
    exit(write_log("Path must be '/xxx/xxx'", _params = sys.argv))
_class_name = tools.upper_first(_path[0].strip())  # 类名，首字母大写
_func_name = _path[1].strip()  # 方法名

# 记录请求参数
if params:
    glob.set_value('params', params)

# 执行函数
try:
    tools.set_stdout()  # 设置中文输出，解决编码问题
    _module = __import__('task.' + _class_name)  # 加载 C 模块
    _module = getattr(_module, _class_name)  # 加载 C._class 模块
    _class = getattr(_module, _class_name)  # 加载 C._class._class 类
    _func = getattr(_class(), _func_name)  # 加载 C._class._class 类的 _func 函数
    _func()  # 调用 _func 函数
except KeyboardInterrupt:
    # 跳过，主动断开
    pass
except ModuleNotFoundError:
    write_log("No module named 'task.{}'".format(_class_name), _params = sys.argv)
except Exception as e:
    details = traceback.format_exc()  # 获取代码执行顺序
    details = details.split('\n')[3:-1]  # 去除无关提示
    positions = []  # 到达报错位置的每个调用节点
    for detail in details[:-1:2]:
        positions.append(detail.strip())

    kwargs = {
        '_msg': {
            'positions': positions,
            'error': details.pop()
        }
    }
    # 日志文件类型，默认为：cli (即：client)
    # 例子：raise ValueError('xxxx', '_file_type', '_remark')
    if tools.is_set(e.args, 1):
        kwargs['_file_type'] = e.args[1]
    kwargs['_params'] = sys.argv
    if tools.is_set(e.args, 2):
        kwargs['_remark'] = e.args[2]
    write_log(**kwargs)
