# coding=utf-8
import json
import logging
from time import sleep

from module.const.version import __version__ as version
from module.rpc import RpcServer
from module.tools import shell
from module.tools.env import dataengine_env
from module.tools.http import HttpClient

__author__ = 'zhangjt'


class Spark2:
    """
    java_opt:
    "-XX:+UseG1GC",
    "-XX:MaxNewSize=3g",
    "-XX:InitiatingHeapOccupancyPercent=35",
    """

    def __init__(self, rpcHandler=None, queue=None):
        self.default_java_opt = [
            "-XX:+UseG1GC",
            "-XX:InitiatingHeapOccupancyPercent=35",
            # "-XX:ConcGCThreads=12" # 20 => Exit status: 1. Diagnostics: Exception from container-launch.
        ]
        self.default_files = [
            "%s/log4j.properties" % dataengine_env.dataengine_conf,
            "%s/hive_database_table.properties" % dataengine_env.dataengine_conf,
        ]
        if rpcHandler is not None:
            self.rpc_server = RpcServer(rpcHandler)
            self.rpc_server.start()
        self.queue = queue

    def submit(self, module="dataengine-core", args="", job_name="", job_id="",
        props=None, conf=None, files=None, java_opt=None,
        jars=None, main_jar=None, hdfs_jars=None):

        if java_opt is None:
            java_opt = self.default_java_opt
        if files is None:
            files = self.default_files
        else:
            files = ["%s/%s" % (dataengine_env.dataengine_conf, f) for f in files]
            files = set(files + self.default_files)

        # default_jars = [
        #     "%s/dataengine-commons-%s.jar" % (dataengine_env.dataengine_lib_path, version),
        # ]
        # if jars is None:
        #     jars = default_jars
        # else:
        #     jars = set(jars + default_jars)

        default_conf = {
            "spark.driver.cores": "2",
            "spark.shuffle.service.enabled": "true",
            "spark.speculation": "true",
            "spark.dynamicAllocation.enabled": "true",
            "spark.dynamicAllocation.initialExecutors": "1",
            "spark.dynamicAllocation.minExecutors": "1",
            "spark.executor.memoryOverhead": "3096",
            "spark.network.timeout": "300s",
            "spark.dynamicAllocation.maxExecutors": dataengine_env.max_executors,
            "spark.speculation.quantile": "0.98",
            "spark.executor.extraJavaOptions": " ".join(java_opt),
            "spark.executorEnv.JAVA_HOME": dataengine_env.my_java_home,
            "spark.yarn.appMasterEnv.JAVA_HOME": dataengine_env.my_java_home,
        }

        if conf is not None:
            default_conf.update(conf)
        conf_arr = ["--conf \"%s=%s\"" % (k, v) for k, v in default_conf.items()]

        default_props = {
            "--name": "%s-%s-%s" % (dataengine_env.env_scope, job_name, job_id),
            "--master": "yarn",
            "--deploy-mode": "cluster",
            # "--queue": "dmpots",
            "--class": "com.mob.dataengine.Bootstrap",
            "--driver-memory": "8g",
            "--executor-memory": "8g",
            "--executor-cores": "4",
            # "--num-executors": "5"
        }
        if self.queue is not None:
            default_props['--queue'] = self.queue

        default_props.update(props)
        props_arr = ["%s %s" % (k, v) for k, v in default_props.items()]

        if main_jar is None:
            main_jar = "%s/%s-%s-jar-with-dependencies.jar" % (
                dataengine_env.dataengine_lib_path, module, version)

        spark_sh = """
            /opt/mobdata/sbin/spark-submit {props}  \\
            --files {files} \\
            {jars} \\
            {main_jar}  \\
            {args}
            """.format(
            props="\t\\\n\t".join(props_arr + conf_arr),
            files=",".join(files),
            jars="" if jars is None and hdfs_jars is None else " --jars %s" % (
                ",".join(
                    map(lambda jar: "%s/%s" % (dataengine_env.dataengine_lib_path, jar), [] if jars is None else jars)
                    + [] if (hdfs_jars is None) else [jar for jar in hdfs_jars]
                )
            ),
            main_jar=main_jar,
            args=args,
        )
        status = shell.submit(spark_sh)
        if status is not 0:
            raise Exception("/opt/mobdata/sbin/spark-submit failed %s" % status)
        return status

    @staticmethod
    def get_job_diagnostics(application_id):
        sleep(1)  # wait for yarn init context
        api = "%s/ws/v1/cluster/apps/%s" % (dataengine_env.yarn_api, application_id)
        logging.info(api)
        app = HttpClient().get(api, 3)
        logging.info(json.dumps(json.loads(app), indent=1))
        return str(json.loads(app)['app']['diagnostics'])


# 不建议使用
class Spark:
    """
    java_opt:
    "-XX:+UseG1GC",
    "-XX:MaxNewSize=3g",
    "-XX:InitiatingHeapOccupancyPercent=35",
    """

    def __init__(self, rpcHandler=None):
        self.default_java_opt = [
            "-XX:+UseG1GC",
            "-XX:InitiatingHeapOccupancyPercent=35",
            # "-XX:ConcGCThreads=12" # 20 => Exit status: 1. Diagnostics: Exception from container-launch.
        ]
        self.default_files = [
            "%s/log4j.properties" % dataengine_env.dataengine_conf,
        ]
        if rpcHandler is not None:
            self.rpc_server = RpcServer(rpcHandler)
            self.rpc_server.start()

    def submit(self, module="dataengine-core", args="", job_name="", job_id="",
        props=None, conf=None, files=None, java_opt=None,
        jars=None, main_jar=None):

        if java_opt is None:
            java_opt = self.default_java_opt
        if files is None:
            files = self.default_files
        else:
            files = ["%s/%s" % (dataengine_env.dataengine_conf, f) for f in files]
            files = set(files + self.default_files)

        # default_jars = [
        #     "%s/dataengine-commons-%s.jar" % (dataengine_env.dataengine_lib_path, version),
        # ]
        # if jars is None:
        #     jars = default_jars
        # else:
        #     jars = set(jars + default_jars)

        default_conf = {
            "spark.shuffle.service.enabled": "true",
            "spark.speculation": "true",
            # "spark.dynamicAllocation.enabled": "true",
            # "spark.dynamicAllocation.initialExecutors": "1",
            # "spark.dynamicAllocation.maxExecutors": "30",
            "spark.speculation.quantile": "0.95",
            "spark.executor.extraJavaOptions": " ".join(java_opt),
            "spark.executorEnv.JAVA_HOME": dataengine_env.my_java_home,
            "spark.yarn.appMasterEnv.JAVA_HOME": dataengine_env.my_java_home,
        }

        if conf is not None:
            default_conf.update(conf)
        conf_arr = ["--conf \"%s=%s\"" % (k, v) for k, v in default_conf.items()]

        default_props = {
            "--name": "%s-%s" % (job_name, job_id),
            "--master": "yarn",
            "--deploy-mode": "cluster",
            # "--queue": "dmpots",
            "--class": "com.mob.dataengine.Bootstrap",
            "--driver-memory": "8g",
            "--executor-memory": "8g",
            "--executor-cores": "4",
            # "--num-executors": "5"
        }
        default_props.update(props)
        props_arr = ["%s %s" % (k, v) for k, v in default_props.items()]

        if main_jar is None:
            main_jar = "%s/%s-%s-jar-with-dependencies.jar" % (
                dataengine_env.dataengine_lib_path, module, version)

        spark_sh = """
            /usr/bin/spark-submit {props}  \\
            --files {files} {jars} \\
            {main_jar}  \\
            {args}
            """.format(
            props="\t\\\n\t".join(props_arr + conf_arr),
            files=",".join(files),
            jars="" if jars is None else " --jars %s" % (
                ",".join(
                    map(lambda jar: "%s/%s" % (dataengine_env.dataengine_lib_path, jar), jars)
                )
            ),
            main_jar=main_jar,
            args=args,
        )
        status = shell.submit(spark_sh)
        if status is not 0:
            raise Exception("/opt/mobdata/sbin/spark-submit failed")
        return status

    @staticmethod
    def get_job_diagnostics(application_id):
        sleep(1)  # wait for yarn init context
        api = "%s/ws/v1/cluster/apps/%s" % (dataengine_env.yarn_api, application_id)
        logging.info(api)
        app = HttpClient().get(api, 3)
        logging.info(json.dumps(json.loads(app), indent=1))
        return str(json.loads(app)['app']['diagnostics'])


if __name__ == '__main__':
    import logging.config
    import os

    logging.config.fileConfig("%s/conf/log.cfg" % os.environ.get('DATAENGINE_HOME'))
    print(Spark2.get_job_diagnostics("application_1531196858550_2624"))
