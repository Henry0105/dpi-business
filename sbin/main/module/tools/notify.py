# coding=utf-8
import json

__author__ = 'zhangjt'

import logging

from module.const.version import __version__ as version
from module.tools import shell, http, cfg_parser
from module.tools.env import dataengine_env
from module.tools.http import HttpClient


class Notifier:
    def __init__(self):
        self.mail = Mail()
        self.mq = MQ()
        self.client = HttpClient()
        self.headers = {"Content-Type": "application/json"}
        # self.api_paht = "/api/mid/callback"

    def send(self, cxt):
        self.mq.send(cxt)
        self.mail.send("%s-%s failed status %s" % (cxt.job_name, cxt.job_id, cxt.status), cxt)

    def send2http(self, cxt, url):
        rp_body = json.dumps({
            "task_id": cxt.task_id,
            "task_result": json.dumps(cxt.__dict__)
        })
        self.client.post(url,
                         data=rp_body,
                         timeout=200,
                         headers=self.headers)
        self.mail.send("%s-%s failed status %s" % (cxt.job_name, cxt.job_id, cxt.status), cxt)


class Mail:
    def __init__(self):
        self.mob_mail_group_url = cfg_parser.get("mob_mail_group_url")
        self.sender_id = cfg_parser.get("sender_id")
        self.groups = cfg_parser.get("groups").split(",")
        self.project_id = cfg_parser.get("project_id")

    # TODO 最好使用Python直接发送
    def send(self, subject, content):
        tmp = ("%s" % content) \
            .replace("\n", "<br>") \
            .replace("\t", "&nbsp;&nbsp;&nbsp;&nbsp;") \
            .replace(" ", "&nbsp;") \
            .replace("'", "") \
            .replace("`", "")

        data_json = {
            "senderId": self.sender_id,
            "groups": self.groups,
            "title": subject[:50],  # title长度不能超过50
            "html": tmp,
            "projectId": self.project_id
        }
        r = http.HttpClient().post(self.mob_mail_group_url, json=data_json)

        if r.status_code is not 0:
            logging.error("mail sender error, status_code=%s, reason=%s", r.status_code, r.reason)


class MQ:
    def __init__(self):
        self.module = "dataengine-utils"
        self.dep_jars_path = 'lib/jars'
        self.dep_jars = ['scala-library-2.12.8.jar', 'log4j-1.2.17.jar', 'slf4j-api-1.7.5.jar']
        self.class_path = ":".join(
            map(lambda jar: "%s/%s/%s" % (dataengine_env.dataengine_home, self.dep_jars_path, jar), self.dep_jars)
        )

    def send(self, content):
        tmp = json.dumps(content.__dict__)
        sh = u"""{my_java_home}/bin/java -cp {main_jar}:{main_conf}:{class_path}  \
                com.mob.dataengine.utils.RabbitMQSender \"{subject}\" \'{content}\'
             """.format(
            subject="dataengine_job_failed_msg",
            main_conf=dataengine_env.dataengine_conf,
            content=tmp,
            main_jar="%s/%s-%s-jar-with-dependencies.jar" % (
                dataengine_env.dataengine_lib_path, self.module, version),
            my_java_home=dataengine_env.my_java_home,
            class_path=self.class_path
        )

        (status, stdout) = shell.submit_with_stdout2(sh)

        if status is not 0:
            logging.error("mq sender error, code=%s, stdout=%s", status, stdout)

    def send2(self, subject, content):
        tmp = json.dumps(content)
        sh = u"""{my_java_home}/bin/java -cp {main_jar}:{main_conf}:{class_path}  \
                com.mob.dataengine.utils.RabbitMQSender \"{subject}\" \'{content}\'
             """.format(
            subject=subject,
            main_conf=dataengine_env.dataengine_conf,
            content=tmp,
            main_jar="%s/%s-%s-jar-with-dependencies.jar" % (
                dataengine_env.dataengine_lib_path, self.module, version),
            my_java_home=dataengine_env.my_java_home,
            class_path=self.class_path
        )

        (status, stdout) = shell.submit_with_stdout2(sh)

        if status is not 0:
            logging.error("mq sender error, code=%s, stdout=%s", status, stdout)

        # credentials = pika.PlainCredentials(username='guest', password='guest')
        # # credentials = pika.PlainCredentials(username='dmp', password='b5049ee39bb038ce83f7')
        # params = pika.ConnectionParameters(host='localhost', port=5672, virtual_host="/", credentials=credentials)
        # # connection = pika.BlockingConnection(params)
        # # channel = connection.channel()
        # # channel.queue_declare(queue='zhangjt', durable=True)
        # # channel.basic_publish(exchange='',
        # #                       routing_key='zhangjt',
        # #                       body="123")
        # # channel.close()
        # # connection.close()
        # connection = pika.BlockingConnection(params)
        # channel = connection.channel()
        # channel.exchange_declare(exchange='direct_logs',
        #                          type='direct')
        # channel.basic_publish(exchange='direct_logs',
        #                       routing_key="info",
        #                       body="{}")
        # connection.close()
