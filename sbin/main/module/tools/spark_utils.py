# encoding:utf-8

import uuid
import json
from module.const import argparse
from module.const.main_class import SPARK_UTILS
from module.rpc import AbstractRpcHandler
from module.tools.spark import Spark2


class SparkUtils(AbstractRpcHandler):
    def __init__(self):
        AbstractRpcHandler.__init__(self)
        self.spark2 = Spark2(rpcHandler=self)
        self.application_id = None
        self.job_id = str(uuid.uuid4())

    def submit(self, args):
        if len(args) is 0:
            args = ["-h"]
        parser = argparse.ArgumentParser(prefix_chars='--')
        parser.add_argument('--job_type', action='store', default=False, help='任务类型')

        namespace, other_args = parser.parse_known_args(args=args)

        json_args = json.dumps(dict([[other_args[idx][2:], other_args[idx+1]] for
                                     idx in range(0, len(other_args), 2)]))

        return self.spark2.submit(
            module="dataengine-utils",
            args='%s \'%s\'' % (namespace.job_type, json_args),
            job_name="spark_utils_{job_type}".format(job_type=namespace.job_type),
            job_id=self.job_id,
            conf={
                "spark.driver.cores": "2"
            },
            props={
                "--executor-memory": "1g",
                "--executor-cores": "1",
                "--class": SPARK_UTILS,
            }
        )
