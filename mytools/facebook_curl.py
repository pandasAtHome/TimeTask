#!/usr/bin/python
# coding: utf-8
import json

# 自定义模块
from mytools import tools


class FacebookCurl(object):
    """
    - 官方文档：https://developers.facebook.com/docs/marketing-api/reference/ad-campaign-group/insights/
    - 调试接口：https://developers.facebook.com/tools/explorer/
    """
    def __init__(self):
        # 获取 Facebook 配置
        config = tools.configs(_section = 'facebook')
        # 私有变量：
        # Facebook API 域名
        self.__host = config['host'] if 'host' in config else 'https://graph.facebook.com'
        # Facebook API 版本
        self.__version = config['version'] if 'version' in config else 'v3.2'
        # Facebook Token
        self.__token = config['token']

        # 保护变量：
        # 默认请求方式
        self._method = 'GET'

        # 公有变量：
        # 广告账号
        self.account = None
        # 请求参数
        self.params = {}

    def get_ads_by_account(self, _params, _type = None):
        """
        通过 广告账号 获取 广告系列
        :param dict _params: 请求参数
        :param str _type: 请求类型 -> ads | adsets | campaigns
        :return:
        :rtype: dict
        """
        if _type is None:
            _type = 'ads'
        if not self.account:
            raise UnboundLocalError('缺少 广告账号', 'facebook')
        self.params.update(_params)
        path = '/act_' + str(self.account) + '/' + _type

        return self.execute(path)

    def get_insights_by_account(self, _params, _level = None):
        """ 通过 广告id 获取 广告信息
        - 示例：
            - 地址：/act_694538267413038/insights?fields=id
            - 可选参数：
                - fields=account_id,campaign_id,campaign_name,adset_id,adset_name,ad_id,ad_name,spend,clicks
                - level=ad
                - action_breakdowns=["action_type","action_reaction"]
                - time_range={'since':'2019-05-08','until':'2019-06-04'}
        :param dict _params: 请求参数
        :param str _level: 查看级别 -> ad | adset | campaign
        :return:
        """
        if _level is None:
            _level = 'ad'
        if not self.account:
            raise UnboundLocalError('缺少 广告账号', 'facebook')
        self.params['level'] = _level
        self.params['action_breakdowns'] = '["action_type","action_reaction"]'
        self.params.update(_params)

        path = '/act_' + str(self.account) + '/insights'

        return self.execute(path)

    def execute(self, _path):
        """
        执行 API 查询
        :param str _path: 请求地址
        :return:
        :rtype: dict
        """
        request_url = self.__host + '/' + self.__version + _path + self.get_params()
        result = tools.curl(request_url, _type = 2)
        return result

    def set_account(self, _account):
        """
        设置 查询的广告账号
        :param _account:
        :return:
        """
        self.account = _account
        return self

    def set_limit(self, _limit):
        """
        设置 每次获取数据条数
        :param _limit:
        :return:
        """
        self.params['limit'] = _limit
        return self

    def set_fields(self, _fields):
        """
        设置 查询字段
        :param _fields:
        :return:
        """
        if 'fields' not in self.params:
            self.params['fields'] = []
        if isinstance(_fields, str):
            self.params['fields'].append(_fields)
        elif isinstance(_fields, list):
            self.params['fields'].extend(_fields)

    def set_time_range(self, _start, _end = None):
        """
        设置时间范围
        :param str _start:
        :param str _end:
        :return:
        """
        if not _start:
            return False
        time_range = {
            'since': _start
        }
        if _end:
            time_range['until'] = _end

        self.params['time_range'] = time_range

    def get_params(self):
        """
        获取请求参数 url
        :return:
        """
        # 检测 token
        if not self.__token:
            raise UnboundLocalError('缺少 Token', 'facebook')
        self.params['access_token'] = self.__token

        if 'limit' not in self.params:
            self.params['limit'] = 20
        lst = []
        for k, v in self.params.items():
            if isinstance(k, int):
                lst.append(v)
            elif isinstance(v, dict):
                lst.append(k + '=' + json.dumps(v))
            elif isinstance(v, list):
                lst.append(k + '=' + ','.join(v))
            elif isinstance(v, int):
                lst.append(k + '=' + str(v))
            else:
                lst.append(k + '=' + v)

        return '?' + ('&'.join(lst))
