#!/usr/bin/python
# coding: utf-8
from __future__ import unicode_literals
import ipdb
import socket
import struct
import json
import IP2Location

# 引入自定义模块
from mytools import tools


def get_country_info(_ip):
    result = {}
    if tools.is_number(_ip):
        _ip = long2ip(int(_ip))
    elif not tools.is_ip(_ip):
        return result
    res = tools.curl('http://ip.taobao.com/service/getIpInfo.php?ip=' + _ip)
    if res:
        res = json.loads(res)
        if 'data' in res:
            res = res['data']
            result['ip'] = res['ip']
            # 国家名称
            if res['country'] and res['country'].lower() != 'xx':
                result['country'] = res['country']
            else:
                result['country'] = None
            # 国家代码
            if res['country_id'] and res['country_id'].lower() != 'xx':
                result['country_code'] = res['country_id']
            else:
                result['country_code'] = None
            # 省份
            if res['region'] and res['region'].lower() != 'xx':
                result['region'] = res['region']
            else:
                result['region'] = None
            # 省份id
            if res['region_id'] and res['region_id'].lower() != 'xx':
                result['region_id'] = res['region_id']
            else:
                result['region_id'] = None
            # 市区
            if res['city'] and res['city'].lower() != 'xx':
                result['city'] = res['city']
            else:
                result['city'] = None
            # 市区id
            if res['city_id'] and res['city_id'].lower() != 'xx':
                result['city_id'] = res['city_id']
            else:
                result['city_id'] = None
            # isp
            if res['isp'] and res['isp'].lower() != 'xx':
                result['isp'] = res['isp']
            else:
                result['isp'] = None
            if res['isp_id'] and res['isp_id'].lower() != 'xx':
                result['isp_id'] = res['isp_id']
            else:
                result['isp_id'] = None

    return result


def get_country_name(_ip):
    """ 获取 国家名称
    https://www.ipip.net/product/client.html
    :param _ip:
    :return:
    """
    if tools.is_number(_ip):
        _ip = long2ip(int(_ip))
    db = ipdb.City(tools.ROOT_PATH + "/P/ipdb/ipipfree.ipdb")
    info = db.find_map(_ip, "CN")
    info['country_code'] = get_country_code(_ip)

    return info


def get_country_code(_ip):
    """ 获取 国家代码
    https://www.ip2location.com/development-libraries/ip2location/python
    :param _ip:
    :return:
    """
    if tools.is_number(_ip):
        _ip = long2ip(int(_ip))
    database = IP2Location.IP2Location(tools.ROOT_PATH + "/P/IP2Location/data/IPV6-COUNTRY.BIN")
    rec = database.get_all(_ip)

    return str(rec.country_short, encoding = 'utf-8')


def ip2long(_ip):
    return struct.unpack('!L', socket.inet_aton(_ip))[0]


def long2ip(_ip):
    return socket.inet_ntoa(struct.pack('!L', _ip))


if __name__ == '__main__':
    tools.set_stdout()
    # data = get_country_name("14.0.157.69")
    # print(data)
    # data = get_country_code("14.0.157.69")
    data = long2ip(int('1850095520'))
    # data = ip2long('110.70.55.160')
    print(data)
