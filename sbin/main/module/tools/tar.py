# coding=utf-8
import shutil

__author__ = 'zhangjt'

import os
import tarfile


def tgz(src, target_name, suffix="tar.gz"):
    """
    :param src: 目标文件或目录
    :param target_name: 文件名称(支持目录/文件名称) example: "./tmp/20185/12/sample"  dirs=>/tmp/20185/12/ file=>sample
    :param suffix: 文件后缀,默认tar.gz
    :return:
    """
    target_dir = os.path.dirname(target_name)
    if target_dir != "" and not os.path.exists(target_dir):
        os.makedirs(target_dir)

    # if target_name.rfind("/") is not -1:
    #     target_name = target_name[target_name.rfind("/") + 1:len(target_name)]
    # print(target_name)
    with tarfile.open("%s.%s" % (target_name, suffix), "w:gz") as tar:
        if isinstance(src, list):
            for _dir in src:
                _add(_dir, tar)
        else:
            _add(src, tar)


def _add(src, tar):
    if not os.path.exists(src):
        os.makedirs(src)
        tar.add(src, arcname=os.path.basename(src))
        shutil.rmtree(src)
    else:
        tar.add(src, arcname=os.path.basename(src))
