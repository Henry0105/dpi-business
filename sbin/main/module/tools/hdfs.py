# coding=utf-8
import logging

from module.tools import shell, cfg_parser

__author__ = 'zhangjt'


def test_success(src):
    status = shell.submit(""" hadoop fs -test -e {src} """.format(src=src))

    if status is not 0:
        logging.info("hadoop fs -test %s failed[status=%s]!" % (src, str(status)))

    return status


# 获取active namenode
def get_active_nn(label):
    nns = cfg_parser.get("hbase_namenode_%s" % label)
    arr = nns.split(",")
    if test_success(arr[0]) is 0:
        return arr[0]
    elif test_success(arr[1]) is 0:
        return arr[1]
    else:
        raise Exception("no active namenode[%s]" % nns)
