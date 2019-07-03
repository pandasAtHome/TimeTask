#!/usr/bin/python
# -*- coding:utf-8 -*-
"""
全局变量模块
    - 1、引入模块： from Core import G
    - 2、初始化： G._init()
    - 3、设置全局变量： G.set_value(key, value)
    - 4、获取全局变量： G.get_value(key, default)
"""


def init():
    """初始化全局变量"""
    global global_dict
    global_dict = {}


def set_value(_key, _value):
    """
    设置全局变量
    :param str _key: 键名
    :param _value: 键值
    :return:
    """
    global_dict[_key] = _value


def get_value(_key = None, _default = None):
    """
    获取全局变量
    :param str _key: 键名
    :param _default: 默认值
    :return:
    """
    try:
        if not _key:
            return global_dict
        else:
            return global_dict[_key]
    except Exception as e:
        return _default


def del_value(_key = None, _sub = None):
    """
    删除全局变量
    :param str _key:
    :param str _sub:
    :return:
    """
    try:
        if not _key:
            return False
        if _sub:
            _sub = _sub.split('.')
            if len(_sub) >= 2:
                del global_dict[_key][_sub[0]][_sub[1]]
            else:
                del global_dict[_key][_sub[0]]
        else:
            del global_dict[_key]
        return True
    except Exception as e:
        return False
