# coding=utf-8
import hashlib
import os
import time
import re
from datetime import datetime, timedelta
from module.tools import shell


class Stopwatch:

    def __init__(self):
        self.start_time = time.time()
        self.end_time = -1

    def format_current_time(self):
        return datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    def duration(self):
        return time.time() - self.start_time

    def watch(self):
        m, s = divmod(self.duration(), 60)
        h, m = divmod(m, 60)
        return "%02dh%02dm%02ds" % (h, m, s)

    def stop(self):
        self.end_time = time.time()


def md5(data):
    data_utf8 = data.encode('utf-8')
    # Refer: https://gist.github.com/javasboy/8747b8a95f87d9f797c34df5a42f30fe
    return hashlib.md5(data_utf8).hexdigest()


def current_day():
    dt = datetime.now()
    return dt.strftime('%Y%m%d')


def yesterday():
    return (datetime.now() + timedelta(days=-1)).strftime("%Y%m%d")


def singleton(cls, *args, **kw):
    instances = {}

    def _singleton():
        if cls not in instances:
            instances[cls] = cls(*args, **kw)
        return instances[cls]

    return _singleton


def scan_files(directory, prefix=None, postfix=None):
    files_list = []
    for root, sub_dirs, files in os.walk(directory):
        for special_file in files:
            if postfix:
                if special_file.endswith(postfix):
                    files_list.append(os.path.join(root, special_file))
            elif prefix:
                if special_file.startswith(prefix):
                    files_list.append(os.path.join(root, special_file))
            else:
                files_list.append(os.path.join(root, special_file))
    return files_list


def unique(list1):
    # init a null list
    unique_list = []

    # traverse for all elements
    for x in list1:
        # check if exists in unique_list or not
        if x not in unique_list and x is not None:
            unique_list.append(x)
    return unique_list


def get_limit(param_output, limit_key="limit", threshold=20000000):
    limit = None
    if param_output is not None and limit_key in param_output and param_output[limit_key] != -1:
        limit = param_output[limit_key]
    elif param_output is not None and limit_key in param_output and param_output[limit_key] == -1:
        pass
    else:
        limit = threshold

    return limit


def get_limit_clause(limit):
    return " limit {limit}".format(limit=limit) if limit is not None and limit != -1 else ""


def build_encrypt_json(param):
    """
    从param提取出加密信息放入新的encrypt结构
    'encrypt': {
        'encrypt_type': 2 // 表示aes加密
        'args': {
          'key': 'kfaf'
          'iv': 'fda'
        }
    }
    :param param:
    :return: encrypt 结构
    """
    result = {}
    if 'encrypt_type' not in param or 'value' not in param['encrypt_type'] or param['encrypt_type']['value'] == 0:
        result['encrypt_type'] = 0
    elif param['encrypt_type']['value'] == 1:
        result['encrypt_type'] = 1
    else:
        result['encrypt_type'] = 2
        result['args'] = {
            'key': param['aes_info']['key'],
            'iv': param['aes_info']['iv']
        }
    return result


def split_files_under_dir(path, threshold=20000000, prefix='split_'):
    if os.path.isdir(path):
        os.chdir(path)
        tmp_full_name = 'full_data'
        shell.submit_with_stdout("cat ./* > %s" % tmp_full_name)
        shell.submit_with_stdout("split -l %d %s %s" % (threshold, tmp_full_name, prefix))
        files = [filename for filename in os.listdir("./") if filename.startswith(prefix)]
        os.chdir("../")
    else:
        shell.submit_with_stdout("split -l %d %s %s_%s" % (threshold, path, path, prefix))
        files = [filename for filename in os.listdir("./") if filename.startswith("%s_%s" % (path, prefix))]
    return files


# 将该目录下的文件进行切分,并返回文件名
def get_split_files(dirname, threshold=20000000, prefix='split_'):
    all_files = []
    for f1 in os.listdir(dirname):
        os.chdir(dirname)
        if os.path.isdir(f1):
            files = split_files_under_dir(f1, threshold, prefix)
            all_files.extend(['%s/%s/%s' % (dirname, f1, filename) for filename in files])
        else:
            files = split_files_under_dir(f1, threshold, prefix)
            all_files.extend(['%s/%s' % (dirname, filename) for filename in files])
        os.chdir("../")
    return all_files


# 过滤掉没有value/value为空的output,对这些output不做导出
def filter_outputs_by_value(outputs, key='value'):
    return [(idx, out) for idx, out in outputs if len(out.get(key, '')) > 0]


def default_encrypt():
    return {'encryptType': 0}


def build_sql(raw_sql_clause):
    (headers, clause) = get_headers_clause(raw_sql_clause)
    # 列内2个空格  列间4个空格
    tmp_clause = "( select {cols} from {clause} ) tmp_clause ".format(cols=",".join([h.strip() for h in headers.split(',')]), clause=clause)
    cols = "CONCAT_WS('{sep}',{colnames}) as device".format(
        sep="".join([' ']*4),
        colnames=",".join(["CONCAT_WS('{sep}',CAST({colname} AS string))".format(sep="".join([' ']*2), colname=h.strip())
                           for h in headers.split(',')])
    )
    return "select {cols} from {clause}".format(cols=cols, clause=tmp_clause)


def get_headers_clause(raw_sql_clause):
    result = re.search('select(.*)from(.*)', raw_sql_clause, re.IGNORECASE)
    print ("headers is ", result.group(1))
    print ("table name filter and other is ", result.group(2))
    headers = result.group(1)
    clause = result.group(2)
    return headers, clause



