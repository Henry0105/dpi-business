# coding=utf-8
import logging
import logging.config
import os

from module.tools import shell


class SparkSQL:
    def __init__(self, queue=None):
        self.queue = queue
        self.cmd = "/opt/mobdata/sbin/spark-sql"

    def submit(self, hql):
        status = shell.submit(self.cmd_with_queue(hql))
        if status is not 0:
            raise Exception("spark-sql execute failed")
        return status

    def submit_with_stdout(self, hql):
        status, stdout = shell.submit_with_stdout(self.cmd_with_queue(hql))
        if status is not 0:
            raise Exception("spark-sql execute failed")
        return status, stdout

    def cmd_with_queue(self, hql):
        if self.queue is None:
            return "%s -e \"%s\"" % (self.cmd, hql)
        else:
            return "%s --queue %s -e \"%s\"" % (self.cmd, self.queue, hql)