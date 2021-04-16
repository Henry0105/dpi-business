# coding=utf-8
import logging
import logging.config
import os

from module.tools import shell
from module.tools.utils import unique

__author__ = 'zhangjt'


class DataEngine:

    def __init__(self):
        self.dataengine_home = os.environ.get('DATAENGINE_HOME')
        logging.config.fileConfig("%s/conf/log.cfg" % self.dataengine_home)
        self.dataengine_lib_path = "%s/lib" % self.dataengine_home
        self.dataengine_tmp = "%s/tmp" % self.dataengine_home

    def create_not_exist(self):
        sql_dir = "%s/conf/sql_scripts/rp_tables_create/" % self.dataengine_home
        l = list()
        # TODO unuse carbondb
        l.append("CREATE DATABASE IF NOT EXISTS rp_dataengine_carbondb")
        l.append("CREATE DATABASE IF NOT EXISTS rp_dataengine")
        l.append("use rp_dataengine")
        for r, d, fs in os.walk(sql_dir):
            for sql_file in fs:
                if ".sql" in sql_file:
                    content = open(os.path.join(r, sql_file), 'r').read()
                    l.append(content)

        if not os.path.exists(self.dataengine_tmp):
            os.makedirs(self.dataengine_tmp)

        sql_str = "hive -f  %s " % os.path.join(self.dataengine_tmp, "_init_.sql")

        open(os.path.join(self.dataengine_tmp, "_init_.sql"), 'w').write(";\n".join(l))

        shell.submit(sql_str)


class Hive:
    def __init__(self):
        pass

    def latest_partition(self, table_name, idx=0):
        (status, stdout) = self.submit_with_stdout("show partitions %s" % table_name)
        if status is 0:
            pars = unique(filter(lambda s: str.strip(s).split("/")[idx], str.split(stdout)))

            if len(pars) is 0:
                return None

            par = pars[len(pars) - 1]
            return str.strip(par)
        else:
            raise Exception("execute %s => %s" % status)

    def drop_partitions(self, table_name, idx=0, version_number=3):
        """
        :param table_name:
        :param idx:
                0:根据day={day}删除,例如:
                day=20180806/plat=1
                day=20180806/plat=2
                day=20180809/plat=1
                day=20180809/plat=2
                day=20180810/plat=1
                day=20180810/plat=2
                day=20180814/plat=1
                day=20180814/plat=2
                day=20180818/plat=1
                day=20180818/plat=2
        :param version_number:
                 0:全部删除
                 1:保留当前分区
                 2:保留最近2个分区
        :return:
        """
        script = "show partitions %s" % table_name
        (status, stdout) = self.submit_with_stdout(script)
        logging.info(stdout)
        logging.info("version_number=%s", str(version_number))
        pars = unique(map(lambda s: str.strip(s).split("/")[idx], str.split(stdout)))
        logging.info("\t".join(pars))
        if len(pars) < 1 or (len(pars) - version_number) < 1:
            return

        logging.info(len(pars) - version_number)
        pars = pars[slice(len(pars) - version_number)]
        if len(pars) > 0:
            script = "ALTER TABLE %s DROP IF EXISTS %s" % (
                table_name,
                ",".join(["PARTITION(%s)" % par for par in pars])
            )
            logging.info(script)
            (status, stdout) = self.submit_with_stdout(script)
            if status is not 0:
                raise Exception("execute %s => %s" % (script, status))

    def submit(self, hql):
        status = shell.submit("hive -e \"%s\"" % hql)
        if status is not 0:
            raise Exception("hive execute failed")
        return status

    def submit_with_stdout(self, hql):
        status, stdout = shell.submit_with_stdout("hive -e \"%s\"" % hql)
        if status is not 0:
            raise Exception("hive execute failed")
        return status, stdout
