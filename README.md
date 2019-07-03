# Task Script(任务脚本)
> ### 简介
* 初衷：处理一些较耗时的任务
* 集成封装了常用的 Mysql、Mongo 方法
    * 由于本人一般用于本地，所以忽略了sql注入等防范；
    * 如有需要，请自行添加


> ### 文件简述
* `config`：配置文件目录
    * 文件后缀必须为：“**.ini**”
* `log`：日志文件目录
    * `cli`：任务报错日志目录
    * `config`：配置文件修改目录
* `mytools`：自封装工具目录
    * `fb_market_api.py`：facebook 市场 API
    * `glob.py`：全局变量模块
    * `ip_find.py`：ip查询模块
    * `mongo.py`：Mongo 模块
    * `mysql.py`：Mysql 模块
    * `tools.py`：常用函数模块
* `task`：任务目录
    * 注：
        * 本目录内的文件名首字母必须大写
        * 推荐使用“**大驼峰**”式命名
* `index.py`：脚本入口文件


> ### 使用说明
* 格式：(假设此项目放在 `/data/time_task` 目录)
    * `python <index.py目录路径>/index.py <类名>/<方法名>[ 参数1=值1[&参数2=值2...]]`
```
# 执行 task 目录下的 Test().test()
$ python /data/time_task/index.py "test/test"

# 带参数
$ python /data/time_task/index.py "test/test" "start=2019-01-01&end=2019-12-31"
```

> ### 注意事项
* 
