import os

from module.const.version import __version__ as version
from module.tools.cfg import cfg_parser
from module.tools.utils import singleton


@singleton
class DataengineEnv:
    def __init__(self):
        self.dataengine_home = os.environ.get('DATAENGINE_HOME')
        self.env_scope = os.environ.get('MID_ENGINE_ENV')
        self.dataengine_lib_path = "%s/lib" % self.dataengine_home
        self.dataengine_conf = "%s/conf" % self.dataengine_home
        self.pom_version = version

        self.dataengine_hdfs_data_home = cfg_parser.get("dataengine_hdfs_data_home")
        self.dataengine_hdfs_tmp = cfg_parser.get("dataengine_hdfs_tmp")
        self.dataengine_data_home = cfg_parser.get("dataengine_data_home")

        self.mobutils_path = cfg_parser.get("mobutils_path")
        self.my_java_home = cfg_parser.get("my_java_home")
        # self.mail_receiver = cfg_parser.get("mail_receiver")
        self.hdfs_namespace = cfg_parser.get("hdfs_namespace")
        self.dataengine_db_name = cfg_parser.get("dataengine_db_name")
        self.max_executors = cfg_parser.get("max_executors")
        self.dataengine_job_mq = cfg_parser.get("dataengine_job_mq")
        # todo need check active namenode
        self.yarn_api = cfg_parser.get("yarn_api")
        self.dfs_root_url = cfg_parser.get("dfs_root_url")


dataengine_env = DataengineEnv()
