# coding=utf-8
import json
import logging
import time
import uuid

from module.tools import shell
from module.tools.env import dataengine_env
from module.tools.http import HttpClient

__author__ = 'zhangjt'


def distcp(src, dest, user="dataengine", queue="root.dataengine", timeout=1800, interval=10, retry_num=3):
    job_name = str(uuid.uuid4())
    yarn_api = YarnApi(dataengine_env.yarn_api)
    cmd = """
            HADOOP_USER_NAME={user} hadoop distcp \\
                -Dmapred.job.queue.name={queue} \\
                -Dmapreduce.job.name={job_name} \\
                -pb -update -delete {src} {dest}
        """.format(src=src, dest=dest, user=user, queue=queue, job_name=job_name)

    status = AppChecker(job_name, yarn_api, timeout, interval, retry_num).submit(cmd)
    return status


class AppChecker:
    RUNNING = 0
    KILLED = 1
    ACCEPTED = 2
    OTHER = 3

    def __init__(self, job_name, yarn_api, timeout=1800, interval=10, retry_num=3):
        self.job_name = job_name
        self.timeout = timeout
        self.yarn_api = yarn_api
        self.interval = interval
        self.retry_num = retry_num
        self.start_time = None
        self.application_id = None

    def check(self):
        if self.application_id is None:
            self.application_id = self.yarn_api.get_appid_by_name(self.job_name)
        if self.application_id:
            state = self.yarn_api.get_app_state(self.application_id).lower()
            if 'running' == state:
                if self.start_time is None:
                    self.start_time = int(time.time())

                elapsed_time = (int(time.time()) - self.start_time)
                if elapsed_time > self.timeout:
                    logging.warn("%s timeout, killing..." % self.application_id)
                    self.yarn_api.kill_app(self.application_id)
                    self.start_time = None
                    self.application_id = None
                    return self.KILLED
                else:
                    return self.RUNNING
            else:
                return self.ACCEPTED
        else:
            return self.OTHER

    def submit(self, cmd):
        p = shell.async_submit(cmd)
        for i in range(self.retry_num):
            while True:
                time.sleep(self.interval)
                process_code = p.poll()
                if process_code == 0:
                    logging.info("distcp succeed")
                    return 0
                elif process_code is not None:
                    logging.error("process error with code %s" % process_code)
                    break
                else:
                    logging.info("still running, %d checking..." % (i + 1))
                    status = self.check()
                    if self.KILLED == status:
                        if i != self.retry_num - 1:  # killed, need restart
                            p.terminate()
                            p = shell.async_submit(cmd)
                            break
                        else:
                            break
                    if self.OTHER == status and i == self.retry_num - 1:
                        break

        p.terminate()
        logging.error("distcp execute too long and killed")
        return 1


class YarnApi:
    def __init__(self, root):
        self.root = root

    def get_app_state(self, application_id):
        api = "%s/ws/v1/cluster/apps/%s/state" % (self.root, application_id)
        logging.info(api)
        res = HttpClient().get(api)
        logging.info(json.dumps(json.loads(res)))
        return json.loads(res)['state']

    def set_app_state(self, application_id, state):
        api = "%s/ws/v1/cluster/apps/%s/state" % (self.root, application_id)
        logging.info(api)
        res = HttpClient().put(api, json={'state': state}, retry_num=3)
        logging.info(json.dumps(json.loads(res._content), indent=1))
        return json.loads(res._content)['state']

    def kill_app(self, application_id):
        killed = 'KILLED'
        state = self.set_app_state(application_id, killed)
        time.sleep(5)  # 需要等一下,这个app的状态才会从 'KILLING' 变为 'KILLED'
        if killed == state:
            logging.info("succeed in killing %s" % application_id)
            return True
        else:
            logging.warn("%s still in state [%s]" % (application_id, state))
            return False

    def get_app_by_name(self, job_name, states=''):
        api = "%s/ws/v1/cluster/apps?states=%s" % (self.root, states)
        logging.info(api)
        info = HttpClient().get(api)
        # logging.info(json.dumps(json.loads(info), indent=1))
        if json.loads(info)['apps']:
            apps = json.loads(info)['apps']['app']
            res = [app for app in apps if app['name'].strip() == job_name]
            if res:
                app = res[0]
                return app
            else:
                logging.warn("job not found")
                return None
        else:
            logging.warn("api return null")
            return None

    def get_appid_by_name(self, job_name):
        cmd = "yarn application -list -appStates ACCEPTED,RUNNING | grep '%s' | awk '{print $1}'" % job_name
        status, appid = shell.submit_with_stdout(cmd)
        if status == 0:
            return appid
        else:
            return None

    def get_running_app_by_name(self, job_name):
        return self.get_app_by_name(job_name, states='running,accepted')

# status = shell.submit("""
#             hadoop fs -test -e {src}/_SUCCESS
#             if [ $? -eq  0 ] ;then
#                 echo 'generate hfile success,distcp begin...'
#                 HADOOP_USER_NAME=hdfs hadoop distcp -Dmapred.job.queue.name=root.dataengine -pb -update -delete {src} {dest}
#             else
#                 echo 'Error! Directory is not exist Or Zero bytes in size'
#                 exit 1
#             fi
#         """.format(src=self.output, dest=cfg_parser.get("%s_hbase_namenode" % label) + "/" + self.output))
# HADOOP_USER_NAME=hdfs hadoop distcp -Dmapreduce.map.speculative=true -Dmapreduce.job.speculative.speculativecap=0.05  -Dmapred.job.queue.name=root.dataengine -pb -update -delete /tmp/dataengine/rp_device_profile_info_20180808 hdfs://bd15-099//tmp/dataengine/rp_device_profile_info_20180808
