# Task Script(����ű�)
> ### ���
* ���ԣ�����һЩ�Ϻ�ʱ������
* ���ɷ�װ�˳��õ� Mysql��Mongo ����
    * ���ڱ���һ�����ڱ��أ����Ժ�����sqlע��ȷ�����
    * ������Ҫ�����������


> ### �ļ�����
* `config`�������ļ�Ŀ¼
    * �ļ���׺����Ϊ����**.ini**��
* `log`����־�ļ�Ŀ¼
    * `cli`�����񱨴���־Ŀ¼
    * `config`�������ļ��޸�Ŀ¼
* `mytools`���Է�װ����Ŀ¼
    * `fb_market_api.py`��facebook �г� API
    * `glob.py`��ȫ�ֱ���ģ��
    * `ip_find.py`��ip��ѯģ��
    * `mongo.py`��Mongo ģ��
    * `mysql.py`��Mysql ģ��
    * `tools.py`�����ú���ģ��
* `task`������Ŀ¼
    * ע��
        * ��Ŀ¼�ڵ��ļ�������ĸ�����д
        * �Ƽ�ʹ�á�**���շ�**��ʽ����
* `index.py`���ű�����ļ�


> ### ʹ��˵��
* ��ʽ��(�������Ŀ���� `/data/time_task` Ŀ¼)
    * `python <index.pyĿ¼·��>/index.py <����>/<������>[ ����1=ֵ1[&����2=ֵ2...]]`
```
# ִ�� task Ŀ¼�µ� Test().test()
$ python /data/time_task/index.py "test/test"

# ������
$ python /data/time_task/index.py "test/test" "start=2019-01-01&end=2019-12-31"
```

> ### ע������
* 
