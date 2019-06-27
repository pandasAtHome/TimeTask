#!/usr/bin/python
# -*- coding:utf-8 -*-
import pymongo
# import sys
# import json
import copy
from bson.son import SON
import traceback

# 自定义模块
from mytools import tools


class Mongo(object):
    def __init__(self, _localhost = None, _has_db = None):
        """
        初始化
        :param bool _localhost: true -> 本地数据库 | false -> 外网数据库
        :param bool _has_db: true -> 检查库不存在,不自动建库 | false -> 不检查库存在,不存在时自动建库
        """
        # 读取 mongo 配置
        _localhost = True if _localhost is None else _localhost
        _has_db = True if _has_db is None else _has_db
        _section = 'mongo' if _localhost else 'mongo2'
        config = tools.configs(_section = _section)
        self.__user = config['user']
        self.__pwd = config['password']
        self.__version = config['version']
        self.__hasDb = _has_db
        self.__skip = 0  # 跳过前 n 条
        self.__limit = 0  # 每次查询数据量
        self.check = False
        self.err = {}
        try:
            # 连接 mongoDb
            self.__conn = pymongo.MongoClient(config['host'], int(config['port']))
            # 连接系统默认数据库admin
            db = self.__conn.admin
            if int(self.__version[0:1]) > 2:
                # mongoDb 3.0+ 版本  ->  SCRAM-SHA-1
                # mongoDb 4.0+ 版本  ->  SCRAM-SHA-1 or SCRAM-SHA-256
                mechanism = 'SCRAM-SHA-1'
            else:
                # mongoDb pre-3.0 版本  ->  MONGODB-CR
                mechanism = 'MONGODB-CR'
            # 通过admin库认证账号权限
            db.authenticate(self.__user, self.__pwd, mechanism = mechanism)
        except Exception as e:
            e.args += ('mongo',)
            raise

    def set_default(self):
        """
        设置默认值
        :return:
        """
        self.__skip = 0  # 跳过前 n 条
        self.__limit = 0  # 每次查询数据量

    def set_col(self, _table):
        """
        选择集合
        :param str _table:
        :return: class Mongo
        """
        _table = self.get_table(_table)
        if not _table:
            exit()
        return self.__conn[_table[0]][_table[1]]

    def select(self, _table, _where = None, _keys = None, _order = None):
        """
        获取单条数据
        :param str _table: 表
        :param dict or None _where: 查询条件
        :param dict or None _keys: 查询字段
        :param dict or None _order: 排序
        :return: False, None or dict
        :rtype dict or bool
        """
        # 选择集合
        conn_col = self.set_col(_table)
        # 查询条件
        pipeline = []
        if _where:
            pipeline.append({'$match': _where})
        if _order:
            pipeline.append({'$sort': SON([(k, v) for (k, v) in _order.items()])})
        pipeline.append({'$skip': 0})
        pipeline.append({'$limit': 1})
        if _keys:
            pipeline.append({'$project': _keys})
        res = {}
        try:
            # 查询结果
            result = conn_col.aggregate(pipeline)
            for r in result:
                res = r
        except Exception as e:
            print(e)
            res = False

        self.set_default()
        return res

    def selects(self, _table, _order = None, _keys = None, _where = None, _group = None):
        """
        获取多条数据
        :param str _table: 表
        :param dict or None _order: 排序
        :param dict or None _keys: 查询字段
        :param dict or None _where: 查询条件
        :param dict _group: 分组
        :return: False, None or list
        :rtype: list[dict] or bool
        """
        # 选择集合
        conn_col = self.set_col(_table)

        # 查询条件
        pipeline = []
        if _where:
            pipeline.append({'$match': _where})
        if _group:
            pipeline.append({'$group': _group})
        if _order:
            pipeline.append({'$sort': SON([(k, v) for (k, v) in _order.items()])})
        if self.__limit:
            pipeline.append({'$skip': self.__skip})
            pipeline.append({'$limit': self.__limit})
        if _keys:
            pipeline.append({'$project': _keys})
        res = []
        try:
            # 查询结果
            result = conn_col.aggregate(pipeline)
            for r in result:
                res.append(r)
        except Exception as e:
            print(e)
            res = False

        self.set_default()
        return res

    def add(self, _table, _param):
        """
        添加单条数据
        :param str _table: 表
        :param dict _param: 要添加的数据
        :return: False or 添加的id
        :rtype: bool or str
        """
        # 选择集合
        conn_col = self.set_col(_table)
        try:
            result = conn_col.insert_one(_param)
            result = result.inserted_id
        except Exception as e:
            # 添加数据失败
            print(e)
            result = False

        self.set_default()
        return result

    def adds(self, _table, _params):
        """
        添加单条数据
        :param str _table: 表
        :param list _params: 要添加的数据
        :return: False or 添加的所有id
        :rtype: bool or list
        """
        # 选择集合
        conn_col = self.set_col(_table)
        try:
            result = conn_col.insert_many(_params)
            result = result.inserted_ids
        except Exception as e:
            # 添加数据失败
            print(e)
            result = False

        self.set_default()
        return result

    def update(self, _table, _where, _param, _limit = None):
        """
        修改
        :param str _table: 表
        :param dict _where: 修改位置
        :param dict _param: 修改内容
        :param bool _limit: 修改数量 True -> 修改一条 | False -> 修改多条
        :return: False or 修改数量
        :rtype: bool or int
        """
        # 选择集合
        conn_col = self.set_col(_table)
        if '$set' not in _param and '$unset' not in _param:
            print("Miss '$set' or '$unset'")
            self.set_default()
            return False
        try:
            _limit = True if _limit is None else _limit
            if _limit:
                result = conn_col.update_one(_where, _param)
            else:
                result = conn_col.update_many(_where, _param)
            # 返回修改数量
            result = result.modified_count
        except Exception as e:
            # 修改数据失败
            print(e)
            result = False

        self.set_default()
        return result

    def delete(self, _table, _where, _limit = None):
        """
        删除
        :param str _table: 表
        :param dict _where: 删除位置
        :param bool _limit: 删除数量 True -> 删除一条 | False -> 删除多条
        :return: False or 删除的数量
        :rtype: bool or int
        """
        # 选择集合
        conn_col = self.set_col(_table)
        try:
            _limit = True if _limit is None else _limit
            if _limit:
                result = conn_col.delete_one(_where)
            else:
                result = conn_col.delete_many(_where)
            # 返回删除数量
            result = result.deleted_count
        except Exception as e:
            # 删除数据失败
            print(e)
            result = False

        self.set_default()
        return result

    def unset_col(self, _table, _where, cols, _limit = None):
        """
        删除字段
        :param str _table: 表
        :param dict _where: 删除位置
        :param list cols: 删除位置
        :param bool _limit: 删除数量 True -> 删除一条 | False -> 删除多条
        :return:
        """
        # 选择集合
        conn_col = self.set_col(_table)
        try:
            _limit = True if _limit is None else _limit
            if _limit:
                conn_col.update_one(_where, )
        except Exception as e:
            pass

    def aggregate(self, _table, _pipeline):
        """
        聚合操作
        :param str _table: 表
        :param list _pipeline: 聚合条件
        :return:
        :rtype: list | dict | bool
        """
        if not _pipeline:
            print("Param '_pipeline' can not be empty")
            self.set_default()
            return False
        # 选择集合
        conn_col = self.set_col(_table)
        try:
            # 查询结果
            result = conn_col.aggregate(_pipeline)
            res = []
            for r in result:
                res.append(r)
        except Exception as e:
            print(e)
            res = False

        self.set_default()
        return res

    def count(self, _table, _where = None, _key = None, _group = None):
        """
        计数
        :param str _table: 表
        :param dict|None _where: 查询条件
        :param str|list|dict|None _key: 去重的键
        :param str|dict|None _group: 分组
        :return: False or 数据量
        :rtype: bool|int|list[dict]
        """
        # 选择集合
        conn_col = self.set_col(_table)

        # 查询条件
        pipeline = []
        if _where:
            pipeline.append({
                '$match': _where
            })
        group1 = {}
        group2 = None
        if _key and _group:
            group2 = {}
            if isinstance(_group, str):
                group1['_id'] = _group
                group2 = '$_id._id'
            else:
                group1 = copy.deepcopy(_group)
                for k in _group:
                    group2[k] = '$_id.' + k

            if isinstance(_key, str):
                group1[_key] = '$' + _key
            elif isinstance(_key, list):
                for item in _key:
                    group1[item] = '$' + str(item)
            else:
                # group1 = {**group1, **_key}  # python 3.5 及以上才支持此语法
                group1.update(_key)
        elif _key:
            if isinstance(_key, str):
                group1 = '$' + _key
            elif isinstance(_key, list):
                group1 = {}
                for item in _key:
                    group1[item] = '$' + str(item)
            else:
                group1 = _key
        elif _group:
            group2 = _group

        # 去重
        if group1:
            pipeline.append({
                '$group': {
                    '_id': group1,
                }
            })
        # 统计
        pipeline.append({
            '$group': {
                '_id': group2,
                'total': {'$sum': 1}
            }
        })
        if self.check:
            self.check = False
            return pipeline
        try:
            # 查询结果
            result = conn_col.aggregate(pipeline)
            if not _group:
                res = 0
                for r in result:
                    res = r['total']
            else:
                res = []
                for r in result:
                    res.append(r)
        except Exception as e:
            print(e)
            res = False

        self.set_default()
        return res

    def distinct(self, _table, _key = None, _where = None):
        """
        去重
        :param str _table: 表
        :param str _key: 去重字段
        :param dict _where: 查询条件
        :return:
        :rtype: bool or list
        """
        # 选择集合
        conn_col = self.set_col(_table)

        try:
            result = conn_col.distinct(_key, _where)
            res = []
            for item in result:
                res.append(item)
        except Exception as e:
            print(e)
            res = False

        self.set_default()
        return res

    def sum(self, _table, _key, _where = None, _group = None):
        """
        求和
        :param str _table: 表
        :param str|list|dict _key: 求和字段
        :param dict _where: 查询条件
        :param str|dict _group: 分组
        :return:
        :rtype: int | dict | list[dict]
        """
        # 选择集合
        conn_col = self.set_col(_table)
        pipeline = []
        if _where:
            pipeline.append({
                '$match': _where
            })
        group = {
            '_id': _group
        }
        if isinstance(_key, str):
            group[_key] = {
                '$sum': '$' + _key
            }
        elif isinstance(_key, list):
            for item in _key:
                group[item] = {
                    '$sum': '$' + item
                }
        elif isinstance(_key, dict):
            for k, v in _key.items():
                group[k] = {
                    '$sum': '$' + v.lstrip('$')
                }
        else:
            print("Param '_key' can not be empty, and its form should be str, list or dict")
            return False
        pipeline.append({
            '$group': group
        })
        try:
            result = list(conn_col.aggregate(pipeline))
            if _group:
                res = []
                for r in result:
                    res.append(r)
            elif isinstance(_key, str):
                res = result[0][_key] if result else 0
            else:
                res = result[0] if result else []
        except Exception as e:
            print(e)
            res = False

        self.set_default()
        return res

    def has_database(self, _db_name):
        """
        判断 数据库是否存在
        :param str _db_name: 数据库名称
        :return: True | False
        :rtype: bool
        """
        # 非 str 类型
        if not isinstance(_db_name, str):
            print("Param '_db_name' is not a string")
            return False
        db_lists = self.__conn.list_database_names()
        if _db_name in db_lists:
            return True

        return False

    def list_databases(self, _empty = None):
        """
        获取所有数据库名称
        :return: 数据库名 or None
        :rtype: list[str]
        """
        # db_lists = self.__conn.list_database_names()
        db_lists = []
        dbs = self.__conn.list_databases()
        if _empty is None:  # 获取所有数据库
            for item in dbs:
                db_lists.append(item['name'])
        elif _empty:  # 获取空数据库
            for item in dbs:
                if item['empty']:
                    db_lists.append(item['name'])
        else:  # 获取非空数据库
            for item in dbs:
                if not item['empty']:
                    db_lists.append(item['name'])
        if db_lists:
            # 过滤 admin, local
            db_lists = list(filter(lambda db: db != 'admin' and db != 'local', db_lists))
            db_lists.sort()

        return db_lists

    def drop_database(self, _db_name):
        """
        删除数据库
        :param str _db_name:
        :return: True or False
        :rtype: bool
        """
        if not isinstance(_db_name, str):
            print("Param '_db_name' is not a string")
            return False

        if self.__hasDb and not self.has_database(_db_name):
            return False

        try:
            self.__conn.drop_database(_db_name)
            result = True
        except Exception as e:
            # 删除数据库失败
            print(e)
            result = False

        self.set_default()
        return result

    def has_collection(self, _table = ''):
        """
        判断 集合是否存在
        :param str _table: 数据库 + 集合名称
        :return: True | False
        :rtype: bool
        """
        _table = self.get_table(_table)
        if not _table:
            print("Miss collection")
            return False
        conn_db = self.__conn[_table[0]]
        col_lists = conn_db.list_collection_names()
        if _table[1] in col_lists:
            return True

        return False

    def list_collections(self, _db_name):
        """
        获取指定数据库下所有集合名称
        :param str _db_name: 数据库
        :return: 集合名 or None
        :rtype: list[str]
        """
        conn_db = self.__conn[_db_name]
        col_lists = conn_db.list_collection_names()
        if col_lists:
            col_lists.sort()
            for (key, col) in enumerate(col_lists):
                if col == 'system.indexes':
                    col_lists.pop(key)
                    continue
                col_lists[key] = _db_name + '.' + col

        return col_lists

    def drop_collection(self, _table):
        """
        删除集合
        :param str _table: 表
        :return: True or False
        :rtype: bool
        """
        # 选择集合
        conn_col = self.set_col(_table)
        try:
            conn_col.drop()
            result = True
        except Exception as e:
            # 删除集合失败
            print(e)
            result = False

        self.set_default()
        return result

    def create_indexs(self, _table, _indexs, _num = None):
        """
        创建索引
        :param str _table: 表
        :param list _indexs: 索引
        :param int _num: <= _num 时才创建索引
        :return: 索引名字 or False
        :rtype: str, list or bool
        """
        if _num is not None and len(self.list_indexs(_table)) > _num:
            return

        # 选择集合
        conn_col = self.set_col(_table)
        indexes = []
        for item in _indexs:
            # 删除 并 获取 _indexs['key']
            index_keys = item.pop('key', None)
            if index_keys is None:
                print("Param _indexs's form is wrong")
                return False
            # 获取 pymongo.IndexModel 的参数：keys
            keys = []
            default_name = []
            if isinstance(index_keys, dict):
                for k, v in index_keys.items():
                    keys.append((k, v))
                    default_name.append(k + '_1')
            elif isinstance(index_keys, list):
                for k in index_keys:
                    keys.append((k, 1))
                    default_name.append(k + '_1')
            index_name = item.pop('name', None)
            index_name = index_name if index_name else '.'.join(default_name)

            # 获取 pymongo.IndexModel 的参数：kwargs
            kwargs = {
                'name': index_name
            }
            for (k, v) in item.items():
                kwargs[k] = v
            indexes.append(pymongo.IndexModel(keys, **kwargs))

        try:
            result = conn_col.create_indexes(indexes)
        except Exception as e:
            # 创建集合失败
            print(e)
            result = False

        return result

    def list_indexs(self, _table):
        """
        获取所有索引
        :param str _table: 表
        :return:
        :rtype: list[dict] or bool
        """
        # 选择集合
        conn_col = self.set_col(_table)
        try:
            # 获取 该集合所有索引
            result = conn_col.list_indexes()
            res = []
            for item in result:
                index = {}
                # 返回 bson.son.SON 类型
                for k, v in item.items():
                    if isinstance(v, SON):
                        index[k] = {}
                        for k1, v1 in v.items():
                            index[k][k1] = v1
                    else:
                        index[k] = v
                res.append(index)
        except Exception as e:
            print(e)
            res = False

        return res

    def drop_indexs(self, _table, _index = None):
        """
        删除索引
        :param str _table:
        :param str _index:
        :return: True or False
        :rtype: bool
        """
        # 选择集合
        conn_col = self.set_col(_table)
        try:
            if _index is None:
                conn_col.drop_indexes()
            elif isinstance(_index, str):
                conn_col.drop_index(_index)
            else:
                print("Param _indexs must be a string")
                return False
            res = True
        except Exception as e:
            print(e)
            res = False

        return res

    def rename_collection(self, _table, _new_name):
        """
        重命名集合
        :param str _table:
        :param str _new_name:
        :return:
        """
        # 选择集合
        conn_col = self.set_col(_table)
        try:
            res = conn_col.rename(_new_name)
        except Exception as e:
            print(e)
            res = False

        return res

    def get_table(self, _table):
        """
        获取 数据库、集合名称
        :param str _table:
        :return:
        :rtype: bool or list
        """
        # 非 str 类型
        if not isinstance(_table, str):
            print("Param _table is not a string")
            return False
        _table = _table.split('.')
        if len(_table) != 2:
            print("Param _table's form is wrong")
            return False
        # 数据库不存在
        if self.__hasDb and not self.has_database(_table[0]):
            return False

        return _table

    def page(self, _page = 1, _count = 10):
        """
        分页
        :param int _page: 页码
        :param int _count: 每页数据量
        :return:
        """
        if _count:
            self.__skip = _count * (_page - 1)
            self.__limit = _count
        return self

    def get_error(self):
        """
        获取报错
        :return: 报错信息
        :rtype: dict
        """
        return self.err

    def close_db(self):
        """
        关闭 Mongo
        """
        self.__conn.close()

    def get_sql(self):
        self.check = True
        return self
