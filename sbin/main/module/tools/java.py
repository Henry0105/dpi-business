# coding=utf-8

from module.const.version import __version__ as version
from module.tools import shell
from module.tools.env import dataengine_env

__author__ = 'zhangjt'


class Java:
    def __init__(self, module="dataengine-utils", jars=None):
        main_jar = "lib/%s-%s.jar" % (module, version)
        final_jars = [main_jar] if jars is None else jars + [main_jar]
        self.class_path = ":".join(
            # jar可能来自于/opt/, 也可能是项目lib
            map(lambda jar: jar if jar.startswith('/') else "%s/%s" % (dataengine_env.dataengine_home, jar),
                final_jars)
        )

    def submit(self, clazz, args):
        """
        :param clazz: 类
        :param args: 参数数组
        :return: code
        """

        sh = """{my_java_home}/bin/java -cp {class_path} {clazz} {args}""".format(
            clazz=clazz, args=" ".join(args),
            class_path=self.class_path,
            my_java_home=dataengine_env.my_java_home
        )

        (status, stdout) = shell.submit_with_stdout(sh)

        if status is not 0:
            raise Exception("java execute failed")

        return stdout
