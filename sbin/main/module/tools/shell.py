# coding=utf-8
import logging

from module.const.common import mock_flag

__author__ = 'zhangjt'
import sys
from subprocess import Popen, PIPE


def submit(cmd):
    """
    提交shell命令,log为标准输出流/标准错误流
    :param cmd: shell command
    :return: int, return 结果
    """
    logging.info("shell => {cmd}".format(cmd=cmd))
    if not mock_flag:
        p = Popen(cmd, stdout=sys.stdout, stderr=sys.stderr, shell=True)
        return p.wait()
    else:
        return 0


def submit_with_stdout(cmd):
    """
    :param cmd: shell 命令
    :return: (int,str,str) => (code,执行日志,异常日志)
    """
    logging.info("shell => {cmd}".format(cmd=cmd))
    if not mock_flag:
        p = Popen(cmd, stdout=PIPE, stderr=sys.stderr, shell=True)
        stdout = p.stdout.read() if p.stdout is not None else ""
        return p.wait(), stdout
    else:
        return 0, ""


# 对比submit_with_stdout不会打印cmd log
def submit_with_stdout2(cmd):
    if not mock_flag:
        p = Popen(cmd, stdout=PIPE, stderr=sys.stderr, shell=True)
        stdout = p.stdout.read() if p.stdout is not None else ""
        return p.wait(), stdout
    else:
        return 0, ""


def submit_stdout_file(cmd, file_name):
    logging.info("shell => {cmd}".format(cmd=cmd))
    f = open(file_name, 'w')
    if not mock_flag:
        p = Popen(cmd, stdout=f, stderr=sys.stderr, shell=True)
        stderr = p.stderr.read() if p.stderr is not None else ""
        return p.wait(), stderr
    else:
        return 0, ""


def submit_with_stderr(cmd):
    """
    :return: (int,str,str) => (code,异常日志)
    """
    logging.info("shell => {cmd}".format(cmd=cmd))
    if not mock_flag:
        p = Popen(cmd, stdout=sys.stdout, stderr=PIPE, shell=True)
        stderr = p.stderr.read() if p.stderr is not None else ""
        return p.wait(), stderr
    else:
        return 0, ""


def async_submit(cmd):
    """
    提交shell命令,log为标准输出流/标准错误流
    :param cmd: shell command
    :return: Popen实例
    """
    logging.info("shell => {cmd}".format(cmd=cmd))
    if not mock_flag:
        return Popen(cmd, stdout=sys.stdout, stderr=sys.stderr, shell=True)
    else:
        return None