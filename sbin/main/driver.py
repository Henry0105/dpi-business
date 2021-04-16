# coding=utf-8
import logging.config
import os
import sys

logging.config.fileConfig("%s/conf/log.cfg" % os.environ.get('DATAENGINE_HOME'))
from module.project import Bootstrap

__author__ = 'zhangjt'

usage = "Usage: python driver.py job|tools"
if __name__ == '__main__':
    if len(sys.argv) < 2:
        print(usage)
        exit(1)

    app = Bootstrap()

    if sys.argv[1] == "job":
        app.run(sys.argv[2], sys.argv[3])
    elif sys.argv[1] == "tools":
        app.tools(sys.argv[2:])
