#!/usr/bin/python
# -*- coding:utf-8 -*-
import sys
import os
import json
import time
import re
import copy
import logging
import unicodedata
import configparser
import codecs
import requests
import subprocess
import pprint
from urllib import parse
from logging import handlers

ROOT_PATH = os.path.dirname(os.path.realpath(__file__)) + '/..'


def logs(_file_name = '', _content = None, _level = 'info', **kwargs):
    """
    日志记录
    :param _file_name: 日志路径
    :param _content: 日志内容
    :param _level: 错误级别
    :param kwargs:
        - str _when: 间隔的时间单位# S 秒
            - M 分
            - H 小时、
            - D 天、
            - W 每星期（interval==0时代表星期一）
            - midnight 每天凌晨
        - int _back_count: 备份文件的个数，如果超过这个个数，就会自动删除
        - int _type: 名字命名方式
            - 1 : _file_name
            - 2 : /yyyy-mm-dd/_file_name
            - 3 : _file_name_yyyy-mm-dd.log
        - bool _console: 是否打印日志内容；默认True
    :return logger

    使用方法：
        F.logs(**kwargs, _content = 'debug')
        F.logs(**kwargs, _content = 'info')
        F.logs(**kwargs, _content = '警告')
        F.logs(**kwargs, _content = '报错')
        F.logs(**kwargs, _content = '严重')
    """
    func = _level.lower()
    # 日志根目录
    log_dir = ROOT_PATH + '/log/'
    _file_name = str(_file_name).strip('/')
    _type = kwargs['_type'] if is_set(kwargs, '_type') else 1
    _console = kwargs['_console'] if is_set(kwargs, '_console') else True
    if _file_name:
        if _type == 2:
            _file_name = get_date() + '/' + _file_name
        elif _type == 3:
            _file_name = _file_name.split('.')
            # 去除 后缀 .log
            last_param = _file_name.pop()
            if last_param != 'log':
                _file_name.append(last_param)
            _file_name = '.'.join(_file_name) + '_' + get_date() + '.log'

        _file_name = log_dir + _file_name

    else:
        _file_name = log_dir + 'other/' + get_date() + '.log'
    # 日志级别关系映射
    level_relations = {
        'debug': logging.DEBUG,
        'info': logging.INFO,
        'warning': logging.WARNING,
        'error': logging.ERROR,
        'critical': logging.CRITICAL
    }
    # 创建文件/目录
    mk_dir(_file_name)
    # 创建 logging 对象
    logger = logging.getLogger(_file_name)
    # 设置日志格式：日期 文件名[line:报错行数] - 报错级别: 报错信息
    fmt = logging.Formatter('[%(asctime)s]'
                            '[pid:%(process)d]'
                            '[%(filename)s:%(lineno)d] => %(levelname)s: %(message)s')
    # 获取并设置：报错级别
    _level = level_relations.get(_level)
    logger.setLevel(_level)

    if _console:
        # 在屏幕上显示
        console = logging.StreamHandler()
        # 设置显示格式
        console.setFormatter(fmt)
        # 把对象加入到 logger
        logger.addHandler(console)

    # 写入日志文件
    # 指定时间间隔，自动生成文件的处理器
    _when = kwargs['_when'] if is_set(kwargs, '_when') else 'D'
    _back_count = kwargs['_back_count'] if is_set(kwargs, '_back_count') else 3
    log = handlers.TimedRotatingFileHandler(filename = _file_name,
                                            when = _when,
                                            backupCount = _back_count,
                                            encoding = 'utf-8')
    # 设置写入格式
    log.setFormatter(fmt)

    # 把对象加入到 logger
    logger.addHandler(log)

    # 记录日志
    getattr(logger, func)(_content)

    # 移除 handler ，防止重复写日志
    if _console:
        logger.removeHandler(console)
    logger.removeHandler(log)

    return True


def mk_dir(_path):
    """
    自动创建目录
    :param _path: 文件路径
    :return: True | False
    """
    # 去除首位空格
    _path = _path.strip()
    # 去除尾部 \ 符号
    _path = _path.rstrip('\\')
    _path = os.path.dirname(_path)

    # 判断路径是否存在
    if not os.path.exists(_path):
        # 不存在，自动创建
        os.makedirs(_path)
        return True
    else:
        # 存在，返回 false
        return False


def get_timestamp(_date = None, _format = None, _ms = False):
    """
    获取当前时间戳 或 日期转时间戳
    :param date|str|None _date: 时间
    :param str|None _format: 格式
    :param bool _ms: 是否获取毫秒：False -> 返回秒 | True -> 返回毫秒
    :return: 返回时间戳(单位：秒)
    """
    if _date:
        if not isinstance(_date, str):
            _date = str(_date)
        if not _format:
            if is_date(_date):
                _format = '%Y-%m-%d %H:%M:%S'
            else:
                _format = '%Y-%m-%d'
        try:
            time_tuple = time.strptime(_date, _format)
        except (TypeError, ValueError):
            return False

        time_stamp = time.mktime(time_tuple)
    else:
        time_stamp = time.time()

    return int(time_stamp)


def get_date(_format = '%Y-%m-%d', _timestamp = None):
    """
    获取当前日期 或 时间戳转日期
    :param _format: 日期格式： %Y -> 年 | %m -> 月 | %d -> 日 | %H -> 时 | %M -> 分 | %S -> 秒
    :param _timestamp: 时间戳
    :return: 指定格式的日期
    """
    if _timestamp is None:
        _timestamp = time.time()

    return time.strftime(_format, time.localtime(_timestamp))


def explode_date(_time = None):
    """
    日期(或 时间戳) 拆分
    :param _time: 要转换的时间
    :return: 集合(年、月、日)
    """
    if not _time:
        return False

    if is_number(_time):
        _time = get_date(_timestamp = _time)

    time_tuple = time.strptime(_time, '%Y-%m-%d')
    return {
        'year': time_tuple.tm_year,
        'month': time_tuple.tm_mon,
        'day': time_tuple.tm_mday,
    }


def get_timezone(_timezone, _time, _data_to_local = None):
    """
    根据时区转换时间戳
    :param int _timezone:
    :param int|date|str _time:
    :param _data_to_local:
    :return:
    """
    if not _timezone:
        _timezone = 0
    else:
        _timezone = int(_timezone)
    if not _time:
        _time = get_timestamp()
    elif not is_number(_time):
        if is_date(_time):
            _time = get_timestamp(_time, '%Y-%m-%d %H:%M:%S')
        else:
            _time = get_timestamp(_time)
    else:
        _time = int(_time)
    if not _timezone or _timezone == 8:
        return _time
    # 时差
    diff = 8 - _timezone
    if _data_to_local is None:
        _data_to_local = True
    result = _time + diff * 3600 if _data_to_local else _time - diff * 3600

    return result


def is_number(_str):
    """
    判断是否为数字
    :param str|int _str: 判断内容
    :return: True or False
    :rtype: bool
    """
    try:
        float(_str)
        return True
    except ValueError:
        pass

    try:
        unicodedata.numeric(_str)
        return True
    except (TypeError, ValueError):
        pass

    return False


def is_array(_data):
    """
    判断是否为数组
    :param _data:
    :return:
    """
    if isinstance(_data, (list, tuple, dict)):
        return True
    return False


def is_set(_data, _key = ''):
    """
        是否存在某 key
        :param _data: 序列 或 字典
        :type: list or dict
        :param str or int _key: 键名
        :return:
        """
    if not _data:
        return False
    try:
        if _data[_key]:
            pass
        return True
    except LookupError:
        return False


def is_json(_data, is_check = True):
    """
    判断是否为 json 格式
    :param _data:
    :param bool is_check: 是否为验证 json 格式： True -> 返回 bool | False -> 返回 bool, list, dict
    :return:
    :rtype: bool, list or dict
    """
    if isinstance(_data, str):
        try:
            result = json.loads(_data, encoding = 'utf-8') if not is_check else True
        except ValueError:
            result = False if is_check else _data
    else:
        result = False if is_check else _data

    return result


def is_date(_date, _format = '%Y-%m-%d %H:%M:%S'):
    """
    判断 日期格式 是否正确
    :param str _date: 日期
    :param str _format: 格式
    :return:
    :rtype: bool
    """
    if get_timestamp(_date, _format):
        return True
    else:
        return False


def is_ip(_ip):
    pattern = '(25[0-5]|2[0-4]\d|[0-1]?\d?\d)(\.(25[0-5]|2[0-4]\d|[0-1]?\d?\d)){3}'
    pattern = re.compile(pattern)  # 预编译，不区分大小写
    if pattern.match(_ip):
        return True
    else:
        return False


def configs(_section = None, _option = None, _params = None, _type = 'select', _file_name = 'common.ini'):
    """
    配置文件操作
    :param str _section: 分组名称
    :param str or None _option: 分组名称
    :param dict, int or str _params: 配置内容
    :param str _type: 操作类型
        # select -> 获取
        # update -> 修改
        # delete -> 删除
    :param str _file_name: 配置文件名
  :return:
    """
    if not _file_name:
        return False

    conf = configparser.ConfigParser()
    path = ROOT_PATH + '/Config/' + _file_name
    # 获取旧配置
    conf.read(path, encoding = "utf-8")
    # 修改/删除内容
    edit = {}
    if _type == 'update':  # 修改
        if not _section:
            print('Miss _section')
            return False
        elif not conf.has_section(_section):  # 添加分组
            conf.add_section(_section)
        edit[_section] = {}
        if _option:
            if is_array(_params):
                _value = json.dumps(_params)
            else:
                _value = str(_params)
            old = conf.get(_section, _option)
            # 无需修改
            if old == _value:
                return True
            edit[_section][_option] = old
            conf.set(_section, _option, _value)
        elif isinstance(_params, dict):
            options = conf.options(_section)
            for _option, _value in _params.items():
                if is_array(_value):
                    _value = json.dumps(_value)
                else:
                    _value = str(_value)
                if conf.has_option(_section, _option):
                    options.remove(_option)
                    old = conf.get(_section, _option)
                    # 无需修改
                    if old == _value:
                        continue
                    edit[_section][_option] = old
                # 修改选项
                conf.set(_section, _option, _value)
            # 删除多余选项
            if options:
                for _option in options:
                    if conf.has_option(_section, _option):
                        # 删除选项
                        edit[_section] = {
                            _option: conf.get(_section, _option)
                        }
                        conf.remove_option(_section, _option)
        else:
            print("_param's type must be dict when without _option")
            return False
    elif _type == 'delete':  # 删除
        edit = {}
        if not _section:
            print('Miss _section')
            return False
        elif conf.has_section(_section):
            if _option:
                if conf.has_option(_section, _option):
                    # 删除选项
                    edit[_section] = {
                        _option: conf.get(_section, _option)
                    }
                    conf.remove_option(_section, _option)
                else:
                    print('without this option: ' + _option)
                    return False
            else:
                # 删除分组
                edit[_section] = conf.items(_section)
                conf.remove_section(_section)
        else:
            print('without this section: ' + _section)
            return False
    else:  # 查询
        result = {}
        # 存在分组
        if not _section:
            for (_section, sections) in conf.items():
                result[_section] = {}
                for (_option, _value) in sections.items():
                    result[_section][_option] = is_json(_value, False)
        elif conf.has_section(_section):
            # 存在选项
            if not _option:
                for (k, v) in conf.items(_section):
                    result[k] = is_json(v, False)
            elif conf.has_option(_section, _option):
                result = is_json(conf.get(_section, _option), False)

        return result

    conf.write(open(path, 'w', encoding = "utf-8"))
    logs('/config/' + _type + '.log', edit, 'info')
    return True


def set_stdout(_type = 'utf-8'):
    """
    设置中文输出
    :param str _type: 输出编码
    :return:
    """
    sys.stdout = codecs.getwriter(_type)(sys.stdout.detach())


def upper_first(_str):
    """
    首字母变大写
    :param str _str:
    :return:
    :rtype str
    """
    if not isinstance(_str, str):
        return False
    return str(_str[0].upper()) + str(_str[1:])


def curl(_url = '', _data = None, _method = 'GET', _type = 1):
    """
    CURL 请求
    :param str _url: 请求地址
    :param list | dict _data: 请求参数
    :param str _method: 请求方法
    :param int _type: 返回数据类型：1 -> json | 2 -> json 解码 | 3 -> 二进制
    :return:
    """
    if _method == 'POST':
        respond = requests.post(_url, data = _data)
    elif _method == 'PUT':
        if _data is not None and _data != '':
            respond = requests.put(_url, data = _data)
        else:
            respond = requests.put(_url)
    elif _method == 'DELETE':
        respond = requests.delete(_url)
    else:
        respond = requests.get(_url, params = _data)

    result = False
    if respond.status_code == 200:
        if _type == 1:
            result = respond.text
        elif _type == 2:
            result = respond.json()
        else:  # 保存图片时使用 3
            result = respond.content
    return result


def request(_path, _data = None):
    """
    执行python脚本
    :param str _path:
    :param dict _data: 参数
    :return:
    """
    cmd = 'python ' + ROOT_PATH + '/index.py "/' + _path.strip('/') + '" '
    if _data:
        if isinstance(_data, dict):
            _data = parse.urlencode(_data)  # url 编码
            cmd += '"' + str(_data) + '"'
        else:
            _data = None
    subprocess.Popen(cmd, shell = True, stdout = subprocess.PIPE, stderr = subprocess.STDOUT)

    # process = subprocess.Popen(cmd, shell = True, stdout = subprocess.PIPE, stderr = subprocess.STDOUT)
    # process.wait()
    # return process.stdout.read().decode('utf-8')


def request_cmd(_cmd, _is_return = None):
    """
    执行 cmd 命令行
    :param str _cmd: 操作命令
    :param bool _is_return: 是否返回结果
    :return:
    """
    result = []
    process = subprocess.Popen(_cmd, shell = True, stdout = subprocess.PIPE, stderr = subprocess.STDOUT)
    if _is_return:
        process.wait()
        result.append(process.stdout.read().decode('utf-8'))
        return result


def url_decode(_data):
    """
    url 参数解析
    :param str or dict _data:
    :return:
    :rtype: dict
    """
    if is_array(_data):
        return _data
    # url 解码，并提取参数
    result = {}
    params = parse.parse_qs(parse.unquote(_data))
    for (k, v) in params.items():
        result[k] = v[0]

    return result


def pr(_data):
    """
    pprint
    :param any _data:
    :return:
    """
    pprint.pprint(_data)


def change_key(_data, _index = 'id', _unset = False):
    """
    更换键名
    :param list _data:
    :param str _index:
    :param bool _unset:
    :return:
    :rtype: dict
    """
    if not _data or not isinstance(_data, list):
        return {}

    result = {}
    for row in _data:
        key = row[_index]
        if _unset:
            del row[_index]
        result[key] = row

    return result


def array_column(_list, _key):
    """
    提取二维 [{}] 中某列值
    :param list _list:
    :param str _key:
    :return:
    :rtype: list
    """
    result = []
    if not _list or not _key or not isinstance(_list, list):
        return result
    for item in _list:
        if _key in item:
            result.append(item[_key])

    return result


def list_unique(_data, _sort = None):
    """
    list 去重
    :param list _data:
    :param bool _sort: True -> 顺序 | False -> 倒序
    :return:
    :rtype: list
    """
    result = list(set(_data))
    if _sort is not None and isinstance(_sort, bool):
        result.sort()
        if not _sort:
            result = reversed(result)

    return list(result)


def list_diff(_list1, _list2):
    """
    获取 两个list 的差集
    :param list _list1:
    :param list _list2:
    :return:
    :rtype: list
    """
    return list(set(_list1).difference(set(_list2)))


def list_diff_all(*args):
    """
    获取 多个list 的差集
    :param list args: 多个 list
    :return:
    :rtype: list
    """
    result = None
    for arg in args:
        result = list(set(result) ^ set(arg))

    return result if result else []


def list_merge(*args):
    """
    获取 list 并集
    :param list args: 多个 list
    :return:
    :rtype: list
    """
    result = None
    for arg in args:
        result = list(set(result).union(set(arg)))

    return result if result else []


def list_intersect(*args):
    """
    获取 list 交集
    :param list args: 多个 list
    :return:
    :type: list
    """
    result = None
    for arg in args:
        if result is None:
            result = arg
        else:
            result = list(set(result).intersection(set(arg)))

    return result if result else []


def set_default_dict(_dict, *args, **kwargs):
    """
    设置 字典默认值
    :param dict _dict:
    :param args:
    :param kwargs:
    :return:
    """
    default = copy.deepcopy(kwargs['default']) if 'default' in kwargs else None
    first = args[0] if args else None
    if first:
        if len(args) > 1:
            if first not in _dict:
                _dict[first] = {}
            set_default_dict(_dict[first], *args[1:], **kwargs)
        elif first not in _dict:
            _dict[first] = default


def exchange_kv(_dict, _func = None):
    """
    字典 k - v 倒置
    :param dict _dict:
    :param _func: 返回值处理
    :return:
    """
    if _func is not None:
        return dict((v, _func(k)) for (k, v) in _dict.items())
    else:
        return dict((v, k) for (k, v) in _dict.items())


def file_get_content(_path, _num = None):
    """
    读取文件内容
    :param str _path: 文件相对路径
    :param int _num: 读取行数；默认读取全部内容
    :return:
    :rtype: list
    """
    _path = ROOT_PATH + '/' + _path.strip('/')
    result = []
    with open(_path, 'r', encoding = 'utf-8') as fpr:
        if _num:
            i = 0
            while i < _num:
                result.append(fpr.readline().strip('\n'))
                i += 1
        else:
            result = fpr.read().splitlines()

    return result


def file_put_content(_path, _content, _encoding = 'utf-8'):
    """
    向文件中写入内容
    :param str _path: 相对路径
    :param str _content: 写入的内容
    :param str _encoding: 编码
    :return:
    """
    _path = ROOT_PATH + '/' + _path.strip('/')
    with open(_path, 'a', encoding = _encoding) as fpw:
        fpw.write(_content)


if __name__ == '__main__':
    pass
