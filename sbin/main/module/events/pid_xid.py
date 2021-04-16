# coding=utf-8

import logging
import uuid

from module.const.argparse import ArgumentParser
from module.tools import shell
from module.tools.spark import Spark2
import datetime
import time
import os


class PidXidLabel:
    def __init__(self):
        self.spark2 = Spark2(rpcHandler=self)

    def submit(self, args):
        if len(args) is 0:
            args = ["-h"]
        parser = ArgumentParser(prefix_chars='--')
        parser.add_argument('--day', action='store', default='', help='day 例如:20180806')
        parser.add_argument('--isFull', action='store', default='false', help='是否跑全量')
        namespace, other_args = parser.parse_known_args(args=args)

        day_date = datetime.datetime.strptime(namespace.day, "%Y%m%d")
        yesterday = str((day_date + datetime.timedelta(days=-1)).strftime("%Y%m%d"))

        param = """{day} {isFull} {yesterday} """.format(
            day=namespace.day,
            isFull=namespace.isFull,
            yesterday=yesterday
        )

        self.spark2.submit(
            "dataengine-utils",
            args=param,
            job_name="pid_xidLabel",
            job_id=str(uuid.uuid4()),
            props={
                "--class": "com.mob.dataengine.utils.pidXid.PidXidLabel"
            },
            jars=["xidlabelsoftsdk-jdk17-1.0.1.0.jar"],
            hdfs_jars=["hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/pid_encrypt.jar"]
        )


class pidXid:
    def __init__(self, args):
        self.spark2 = Spark2(rpcHandler=self)
        self.local_path, self.day, self.read_table, other_args_1 = self.get_step1_args(args)
        self.ip_211, self.remote_path_211, other_args_2 = self.get_step2_args(other_args_1)
        self.hdfs_path, self.isFull = self.get_step3_step4_args(other_args_2)

    # step1 从hive表mobdi_test.pid_full拿到每日增量的数据并落盘
    @staticmethod
    def get_step1_args(args):
        if len(args) is 0:
            args = ["-h"]
        parser = ArgumentParser(prefix_chars='--')
        parser.add_argument('--day', action='store', default='', help='day 例如:20180806')
        parser.add_argument('--local_path', action='store', help='项目部署集群存放增量phone数据的路径')
        parser.add_argument('--read_table', action='store', default='', help='数据来源表')

        namespace, other_args = parser.parse_known_args(args=args)
        logging.info('step1_args args:')
        logging.info(namespace)
        logging.info(other_args)

        return namespace.local_path, namespace.day, namespace.read_table, other_args

    @staticmethod
    def get_step2_args(args):
        if len(args) is 0:
            args = ["-h"]
        parser = ArgumentParser(prefix_chars='--')
        parser.add_argument('--ip_211', action='store', help='211集群的ip地址')
        parser.add_argument('--remote_path_211', action='store', help='211集群存放全量pid路径')

        namespace, other_args = parser.parse_known_args(args=args)
        logging.info('step2_args args:')
        logging.info(namespace)
        logging.info(other_args)
        return namespace.ip_211, namespace.remote_path_211, other_args

    @staticmethod
    def get_step3_step4_args(args):
        if len(args) is 0:
            args = ["-h"]
        parser = ArgumentParser(prefix_chars='--')
        parser.add_argument('--hdfs_path', action='store', help='hdfs地址')
        parser.add_argument('--isFull', action='store', help='是否全量跑')

        namespace, other_args = parser.parse_known_args(args=args)
        logging.info('step3_step4_args args:')
        logging.info(namespace)
        logging.info(other_args)
        return namespace.hdfs_path, namespace.isFull

    def encrypt_remote(self):

        local_phone_path = self.local_path + "/phone/" + self.day + ".txt"
        remote_phone_path = self.remote_path_211 + "/phone.txt"
        phone_xid_tmp_path = self.remote_path_211 + "/dst/phone.txt.xid"
        phone_xid_path = self.remote_path_211 + "/phone_xid/" + self.day + ".txt"

        # 从本地scp到远程211集群
        local_to_remote_cmd = "scp {_local_phone_path} datax_test@{_ip}:{_remote_phone_path}" \
            .format(_local_phone_path=local_phone_path,
                    _ip=self.ip_211, _remote_phone_path=remote_phone_path)
        status1, stdout1 = shell.submit_with_stdout(local_to_remote_cmd)
        if status1 is not 0:
            raise Exception("phone to 211 execute failed")

        # 启动加密脚本加密
        encrypt_cmd = "ssh datax_test@{_ip} \"sh /home/datax_test/xid-server/run_guhj.sh\" ".format(_ip=self.ip_211)
        status2, stdout2 = shell.submit_with_stdout(encrypt_cmd)
        if status2 is not 0:
            raise Exception("encrypt in 211 execute failed")

        # 从临时目录转移到正式目录
        cp_cmd = "ssh datax_test@{_ip} \"cp {phone_xid_tmp_path} {phone_xid_path}\" " \
            .format(_ip=self.ip_211, phone_xid_tmp_path=phone_xid_tmp_path,
                    phone_xid_path=phone_xid_path)
        status3, stdout3 = shell.submit_with_stdout(cp_cmd)
        if status3 is not 0:
            raise Exception("remote cp in 211 execute failed")

    def load_phone_to_local_path(self):
        timestamp = str(int(time.mktime(time.strptime(self.day, "%Y%m%d"))))
        local_file_path = "{_output_path}/phone/{_day}.txt".format(_output_path=self.local_path, _day=self.day)
        if os.path.exists(local_file_path) and os.path.getsize(local_file_path) > 0:  # 如果本地文件存在且大于0那就跳过
            return
        hql = "/opt/mobdata/sbin/spark-sql -e " \
              "'select phone from {_read_table} " \
              "where update_time>={_timestamp} ' " \
              "> {_local_file_path}" \
            .format(_local_file_path=local_file_path, _read_table=self.read_table, _timestamp=timestamp)
        status, stdout = shell.submit_with_stdout(hql)
        if status is not 0:
            raise Exception("load pid_full execute failed")
        return status, stdout

    def cp_xid_to_local_hdfs(self):
        remote_phone_xid_path = self.remote_path_211 + "/phone_xid/" + self.day + ".txt"
        local_phone_xid_path = self.local_path + "/phone_xid/" + self.day + ".txt"
        hdfs_path = self.hdfs_path

        xid_to_local_cmd = "scp datax_test@{_ip}:{_xid_file} {_local_path}" \
            .format(_ip=self.ip_211, _xid_file=remote_phone_xid_path,
                    _local_path=local_phone_xid_path)

        status1, stdout1 = shell.submit_with_stdout(xid_to_local_cmd)
        if status1 is not 0:
            raise Exception("cp phone_xid to local execute failed")

        hdfs_file_path = hdfs_path + "/" + self.day + ".txt"
        hdfs_file_exists_cmd = "hdfs dfs -ls {_hdfs_file_path}" \
            .format(_hdfs_file_path=hdfs_file_path)
        status2, stdout2 = shell.submit_with_stdout(hdfs_file_exists_cmd)
        if status2 is 0:
            shell.submit_with_stdout("hdfs dfs -rm {_hdfs_file_path}".format(_hdfs_file_path=hdfs_file_path))

        local_to_hdfs_cmd = "hdfs dfs -put {_local_path} {_hdfs_path}" \
            .format(_local_path=local_phone_xid_path, _hdfs_path=hdfs_path)
        status3, stdout3 = shell.submit_with_stdout(local_to_hdfs_cmd)
        if status3 is not 0:
            raise Exception(" local to hdfs execute failed")

    def get_yesterday(self):
        day = datetime.datetime.strptime(self.day, "%Y%m%d")
        yesterday = day + datetime.timedelta(days=-1)
        return str(yesterday.strftime("%Y%m%d"))

    def submit(self):

        spark_args = """{day} {full} {_hdfs_path} {_yesterday}""". \
            format(day=self.day, full=self.isFull, _hdfs_path=self.hdfs_path, _yesterday=self.get_yesterday())

        self.spark2.submit(
            "dataengine-utils",
            args=spark_args,
            job_name="pid_xid",
            job_id=str(uuid.uuid4()),
            props={"--class": "com.mob.dataengine.utils.pidXid.PidXid"}
        )

    def start_up(self):
        self.load_phone_to_local_path()  # 第一步，增量phone数据load到本地目录
        self.encrypt_remote()  # 第二步，将数据scp到远程211集群，启动远程加密脚本加密
        self.cp_xid_to_local_hdfs()  # 第三步，将远程加密后的数据取回本地, 上传至hdfs
        self.submit()  # 第四步，phone加密成pid与昨日数据union后写入hive表
