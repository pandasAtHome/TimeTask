#!/usr/bin/python
# -*- coding:utf-8 -*-
import pymysql
import json
import os
import re
import inspect

# 自定义模块
from mytools import tools
from mytools import glob


# 自定义报错：配置错误
class ConfigError(Exception):
    pass


# 自定义报错：空值错误
class EmptyError(Exception):
    pass


class MySql(object):
    def __init__(self, conf = None):
        # Core.__init__(self)
        """
        初始化
        :param dict conf: 配置(必须内容：host, username, password, database)
        """
        if not conf:
            # 获取 mysql 配置
            conf = tools.configs(_section = 'mysql')
        if ('host' not in conf) or ('username' not in conf) or ('password' not in conf) or ('database' not in conf):
            raise ConfigError('Miss host,username,password or database', 'mysql')
        self.__host = conf['host']
        self.__username = conf['username']
        self.__password = conf['password']
        self.__database = conf['database']
        self.__port = int(conf['port']) if 'port' in conf else 3306
        self.__charset = conf['charset'] if 'charset' in conf else 'utf8mb4'
        self.__conn = None
        self.__cur = None
        self.__sql = None
        self.__where = ''
        self.__order = ''
        self.__group = ''
        self.__param = []
        self.__limit = ''
        self.__check = False
        self.__error = {}

        try:
            self.__conn = pymysql.connect(host = self.__host, user = self.__username, passwd = self.__password,
                                          port = self.__port, db = self.__database, charset = self.__charset)
            self.__cur = self.__conn.cursor()
            self.set_default()
        except Exception as e:
            # mysql连接错误
            e.args += ('mysql',)
            raise

    def set_default(self):
        self.__sql = None
        self.__where = ''
        self.__order = ''
        self.__group = ''
        self.__param = []
        self.__limit = ''
        self.__check = False

    def select(self, _table, _where = None, _key = None, _order = None, _pos = 0):
        """
        获取一条数据
        :param str _table: 表名
        :param str|dict|None _where: 查询条件
        :param str|None _key: 查询字段
        :param str _order: 排序
        :param int _pos: 位置(默认取第一个)
        :return: False | None | dict
        :rtype: dict
        """

        # 获取查询条件
        self.get_where(_where)
        # 字段名
        if not _key:
            _key = '*'
        # 排序
        self.get_order(_order)

        self.__sql = 'SELECT ' + _key + ' FROM ' + str(_table) + self.__where + self.__order + ' LIMIT ' + str(
            _pos) + ',1'
        res = self.query_sql('s')

        return res

    def selects(self, _table, _order = None, _key = None, _where = None, _group = None):
        """
        查询多条数据
        :param str _table: 表
        :param str|None _order: 排序
        :param str|None _key: 查询字段
        :param str| dict _where: 查询条件
        :param str _group: 分组
        :return: False | None | list
        :rtype: list
        """
        # 获取查询条件
        self.get_where(_where)
        # 字段名
        if not _key:
            _key = '*'
        # 排序
        self.get_order(_order)
        # 分组
        self.get_group(_group)
        # 查询数据量
        self.__sql = 'SELECT ' + _key + ' FROM ' + str(
            _table) + self.__where + self.__group + self.__order + self.__limit
        res = self.query_sql('se')

        return res

    def update(self, _table, _where = None, _param = None, _limit = True):
        """
        修改
        :param str _table: 表
        :param str|dict _where: 修改位置
        :param str|dict _param: 修改内容
        :param bool _limit: 修改数量 True -> 1 条 | False -> 多条
        :return: False | 修改数量
        :rtype: bool
        """
        # 获取查询条件
        self.get_where(_where, True)
        if not _param:
            raise ValueError('Param \'_param\' can not be empty', 'mysql')
        if not isinstance(_param, dict):
            raise TypeError(_param, 'mysql')

        # 获取修改内容
        lst = []
        for k, v in _param.items():
            if isinstance(k, int):
                lst.append(v)
            else:
                if not v:
                    lst.append('`' + k + '`=NULL')
                elif tools.is_array(v):
                    lst.append('`' + k + '`=\'' + json.dumps(v) + '\'')
                else:
                    lst.append('`' + k + '`=\'' + str(v) + '\'')
        _param = ','.join(lst)

        _limit = ' LIMIT 1' if _limit else ''

        self.__sql = 'UPDATE ' + _table + ' SET ' + _param + self.__where + _limit
        res = self.query_sql('u')

        return res

    def add(self, _table, _param = None):
        """
        添加单条数据
        :param str _table: 表
        :param dict _param: 添加内容
        :return: False | 新增id
        :rtype: bool or int
        """
        if not _param:
            return False
        if not isinstance(_param, dict):
            raise TypeError(_param, 'mysql')
        # 获取添加内容
        keys = []
        values = []
        for k, v in _param.items():
            keys.append('`' + k + '`')
            if not v:
                values.append('NULL')
            elif tools.is_array(v):
                values.append('\'' + re.escape(json.dumps(v)) + '\'')
            else:
                values.append('\'' + str(v) + '\'')

        self.__sql = 'INSERT INTO ' + _table + ' ( ' + (','.join(keys)) + ') VALUES (' + (','.join(values)) + ')'
        result_bool = self.query_sql('a')

        return result_bool

    def adds(self, _table, _params = None):
        """
        添加多条数据
        :param str _table: 表
        :param dict _params: 添加内容
        :return: False | 新增id
        :rtype: bool or list
        """
        if not _params:
            return False
        # 获取添加内容
        keys = []
        for k, v in _params[0].items():
            keys.append('`' + k + '`')

        values = []
        value = []
        for _param in _params:
            if not isinstance(_param, dict):
                tools.logs('/mysql/error',
                           {'file_type': 'mysql', 'message': _param},
                           'error',
                           _type = 3)
                continue
            for k, v in _param.items():
                if not v:
                    value.append('`NULL')
                elif tools.is_array(v):
                    value.append('\'' + re.escape(json.dumps(v)) + '\'')
                else:
                    value.append('\'' + v + '\'')
            values.append('(' + (','.join(value)) + ')')

        self.__sql = 'INSERT INTO ' + _table + ' ( ' + (','.join(keys)) + ') VALUES ' + (','.join(values))
        res = self.query_sql('a')

        return res

    def delete(self, _table, _where = None, _limit = True):
        """
        删除
        :param str _table: 表
        :param str|dict _where: 删除位置
        :param bool _limit: 删除数量 True -> 1 条 | False -> 多条
        :return: False | 删除数量
        :rtype: bool or int
        """
        # 获取查询条件
        self.get_where(_where, True)
        _limit = ' LIMIT 1' if _limit else ''

        self.__sql = 'DELETE FROM ' + str(_table) + ' WHERE ' + self.__where + _limit
        res = self.query_sql('d')

        return res

    def count(self, _table, _where = None):
        """
        数据量统计
        :param str _table: 表
        :param str|dict _where: 查询条件
        :return: 总数据量
        :rtype: int
        """
        # 获取查询条件
        self.get_where(_where)
        self.__sql = 'SELECT COUNT(*) total FROM ' + str(_table) + self.__where
        result = self.query_sql('s')
        res = result['total'] if result else 0

        return res

    def sum(self, _table, _key = None, _where = None):
        """
        求和
        :param str _table: 表
        :param str _key: 求和字段
        :param str| dict _where: 查询条件
        :return: 总和
        :rtype: int
        """
        if not _key:
            return False
        # 获取查询条件
        self.get_where(_where)
        self.__sql = 'SELECT SUM(' + str(_key) + ') total FROM ' + str(_table) + self.__where
        result = self.query_sql('s')
        res = result['total'] if result else 0

        return res

    def page(self, _page, _count = 10):
        """
        分页
        :param int _page: 页码
        :param int _count: 每页数据量
        :return:
        """
        if _count:
            self.__limit = ' LIMIT ' + str(_count * (_page - 1)) + ',' + str(_count)
        return self

    def get_sql(self, _check = True):
        """
        获取 SQL
        :param bool _check: 是否返回 SQL 语句
        :return:
        """
        self.__check = _check
        return self

    def get_where(self, _where = None, _alter = False):
        """
        获取查询条件
        :param dict|str _where: 条件
        :param bool _alter: 是否记录日志
        :return:
        """
        if _where:
            if tools.is_array(_where):
                lst = []
                for i, row in _where.items():
                    # 跳过数字
                    if tools.is_number(i):  # 处理 i = 'id=1'
                        lst.append(row)
                        continue
                    else:
                        i = str(i)
                        if i.find('.') >= 0:  # 处理 i = 'A.id'
                            columns = i.split('.')
                            key = columns[0] + '.' + '`' + str(columns[1]) + '` '
                        else:  # 处理 i = 'id'
                            key = '`' + str(i) + '` '
                    if isinstance(row, dict):  # 处理 in , like , > , < , >= , <= , != , <>
                        for j, col in row.items():
                            if j.lower() == 'in' and col:  # 处理 in
                                if isinstance(col, list):  # where['name']['in'] = []
                                    new = []
                                    for item in col:
                                        new.append('\'' + str(item) + '\'')
                                    value = j + ' (' + (','.join(new)) + ')'
                                elif isinstance(col, str):  # where['name']['in'] = 'abc'
                                    col = col.lstrip('(')
                                    col = col.rstrip(')')
                                    value = j + ' (' + col + ')'
                                elif tools.is_number(col):  # where['name']['in'] = 1
                                    value = j + ' (' + str(col) + ')'
                                else:
                                    continue
                            elif j.lower() == 'like' and col and not tools.is_array(col):  # 处理 like
                                value = j + ' \'' + str(col).strip("'") + '\''
                            elif j in ['>', '<', '=', '>=', '<=', '!=', '<>'] and not tools.is_array(col):
                                # 处理 > , < , = , >= , <= , != , <>
                                value = j + ' \'' + str(col) + '\''
                            else:
                                continue

                            lst.append(key + value)
                    else:  # 处理 =
                        if isinstance(row, str):  # str 类型
                            value = '= \'' + row + '\''
                            lst.append(key + value)
                        elif isinstance(row, int):
                            value = '= ' + str(row)
                            lst.append(key + value)
                        else:
                            continue

                _where = ' && '.join(lst)

            if _where:
                self.__where = ' WHERE ' + _where
        elif _alter:
            raise EmptyError('Miss _where', 'mysql')

    def get_order(self, _order):
        """
        获取 排序
        :param str _order: 排序
        :return:
        """
        if _order:
            self.__order = ' ORDER BY ' + _order
        else:
            self.__order = ''

    def get_group(self, _group):
        """
        获取 分组
        :param str _group: 分组
        :return:
        """
        if _group:
            self.__group = ' GROUP BY ' + _group
        else:
            self.__group = ''

    def query_sql(self, _type = 's', _sql = ''):
        """
        执行 SQL
        :param str _type: 执行类型 s -> 查询单条 | se -> 查询多条 | u -> 修改 | a -> 添加 | d -> 删除
        :param str _sql: SQL 语句
        :return:
        :rtype: bool, int, list or dict
        """
        if _sql:
            self.__sql = _sql

        if self.__check:
            result = self.__sql
            # 恢复默认值
            self.set_default()
            return result

        try:
            rows = self.__cur.execute(self.__sql)
            if _type == 's':
                # 执行 SQL 语句
                # 获取单条数据
                data = self.__cur.fetchone()
                result = {}
                if data:
                    cols_list = []
                    for field in self.__cur.description:
                        cols_list.append(field[0])

                    for k, v in enumerate(data):
                        result[cols_list[k]] = v
            elif _type == 'se':
                # 获取数据
                data = self.__cur.fetchall()
                result = []
                if data:
                    cols_list = []
                    for field in self.__cur.description:
                        cols_list.append(field[0])

                    for item in data:
                        arr = {}
                        for k, v in enumerate(item):
                            arr[cols_list[k]] = v
                        result.append(arr)
            elif _type == 'u' or _type == 'a' or _type == 'd':
                self.__conn.commit()
                if _type == 'a':
                    result = self.__cur.lastrowid
                else:
                    result = rows
            else:
                result = False
        except Exception as e:
            tools.logs('/mysql/error',
                       {'file_type': 'mysql', 'message': e, 'sql': self.__sql},
                       'error',
                       _type = 3)
            result = False

        # 恢复默认值
        self.set_default()

        return result

    def py_log(self, _type, **kwargs):
        """
        python 日志表
        :param str _type: 日志类型
        :param kwargs:
        :return:
        """
        # 报错信息
        if 'error' in kwargs:
            if not tools.is_array(kwargs['error']):
                kwargs['error'] = [kwargs['error']]
        else:
            kwargs['error'] = None

        # 请求参数
        if 'params' in kwargs:
            kwargs['params'] = tools.url_decode(kwargs['params'])
        else:
            kwargs['params'] = None

        # 备注
        if 'remark' not in kwargs:
            kwargs['remark'] = None
        elif isinstance(kwargs['remark'], str):
            kwargs['remark'] = {'msg': kwargs['remark']}

        # 记录日志
        self.add('_log_python_request', {
            'pid': os.getpid(),
            'type': _type,
            'params': kwargs['params'],
            'error': kwargs['error'],
            'remark': kwargs['remark'],
            'create_time': glob.get_value('create_time'),
            'update_time': tools.get_date('%Y-%m-%d %H:%M:%S'),
        })

    def err_log(self, remark = None):
        """
        记录执行错误:正常情况不会错误的地方出错了
        :param remark:
        :return:
        """
        msg = inspect.stack()[-2]
        param = {
            'path': re.escape(msg),
            'add_time': tools.get_date('%Y-%m-%d %H:%M:%S')
        }
        if remark:
            param['remark'] = re.escape(remark) if isinstance(remark, str) else remark
        self.add('_log_error', param)

    def get_last_id(self):
        """
        获取最新增加的id
        :return:
        """
        return self.__cur.lastrowid

    def close_db(self):
        self.__conn.close()
        self.set_default()

# if __name__ == '__main__':
#     M = MySQL()
#     sql = 'SELECT * FROM `game` WHERE `id`=%s'
#     param = [10500]
#     data = M.cur.execute(sql, param)
#     print(data)
