# -*- coding: utf-8 -*-
from mytools import glob
from mytools import tools


VERSION = 1.0

# 请求参数解析：字典类型
params = glob.get_value('params')
if params:
    params = tools.url_decode(params[0])
