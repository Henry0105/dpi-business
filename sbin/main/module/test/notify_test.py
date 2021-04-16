import logging.config
import os

from module import DataengineEnv, Notifier, Bootstrap
from module.project import JobContext

logging.config.fileConfig("%s/conf/log.cfg" % os.environ.get('DATAENGINE_HOME'))
if __name__ == '__main__':
    jc = JobContext(
        "file:///Users/juntao/src/Yoozoo/dataengine/docs/jobs/filter/device_crowd_secondary_filter.json", Bootstrap())
    jc.end_job()
    Notifier(DataengineEnv()).send(jc)
