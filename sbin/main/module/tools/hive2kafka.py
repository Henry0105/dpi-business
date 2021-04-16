# coding=utf-8
import logging
import uuid

from module.const.argparse import ArgumentParser
from module.tools.spark import Spark2


class Hive2Kafka:
    def __init__(self, args):
        self.spark2 = Spark2(rpcHandler=self)
        self.spark_args, other_args = self.get_spark_args(args)
        self.props, self.conf = self.get_spark_props(other_args)

    @staticmethod
    def get_spark_args(args):
        """
        spark-submit args部分参数解析
        :return args_list  解析完的参数专程list输出
                e.g. ['--topic 1 --sql "select * from t1" --bootstrap_server host1:port1,host2:port2']
        :return other_args 剩余参数
        """
        parser = ArgumentParser(prefix_chars='--')
        parser.add_argument('--job_name', action='store', default='', help='任务 名称')
        parser.add_argument('--bootstrap_server', action='store', required=True,
                            help='kafka bootstrap-server,host1:port1,host2:port2...')
        parser.add_argument('--topic', action='store', required=True, help='kafka 主题')
        parser.add_argument('--sql', action='store', required=True, help='sql语句')
        parser.add_argument('--numPartitions', action='store', default=3, help='并发数,kafka生产者个数')
        parser.add_argument('--kafka_producer_conf', action='store', default='', help='kafka生产者配置,k1=v1,k2=v2...')
        parser.add_argument('--udfs', action='store', default='', help='udf对应的类名和函数名,k1=v1,k2=v2...')
        parser.add_argument('--udf_jars', action='store', default='', help='udf对应的jar包,<jar1>,<jar2>...')

        namespace, other_args = parser.parse_known_args(args=args)
        logging.info('spark args:')
        logging.info(namespace)
        logging.info(other_args)

        # 参数解析完转换成list,sql需要单独处理下
        spark_args = """  --bootstrapServer {bootstrap_server} \\
                    --topic {topic} \\
                    --sql \"{sql}\" \\
                    {job_name} \\
                    {numPartitions} \\
                    {kafka_producer_conf} \\
                    {udfs} \\
                    {udf_jars}
                """.format(
            bootstrap_server=namespace.bootstrap_server,
            topic=namespace.topic,
            sql=namespace.sql,
            job_name=("--job_name %s" % namespace.job_name if namespace.job_name else ""),
            numPartitions=("--numPartitions %s" % namespace.numPartitions if namespace.numPartitions else ""),
            kafka_producer_conf=
            ("--kafkaProducerConf %s" % namespace.kafka_producer_conf if namespace.kafka_producer_conf else ""),
            udfs=("--udfs %s" % namespace.udfs if namespace.udfs else ""),
            udf_jars=("--udfJars %s" % namespace.udf_jars if namespace.udf_jars else "")
        )
        return spark_args, other_args

    @staticmethod
    def get_spark_props(args):
        """
        解析spark-submit的配置参数,未配置则采用默认参数
        :return: props  spark-submit props部分
        :return: conf   spark-submit conf部分
        """
        # props部分
        parser = ArgumentParser(prefix_chars='--')
        # parser.add_argument('--driver_cores', action='store', default="3", help='spark driver核数')
        parser.add_argument('--driver_memory', action='store', default="6G", help='spark driver内存')
        parser.add_argument('--executor_cores', action='store', default="4", help='spark executor核数')
        parser.add_argument('--executor_memory', action='store', default="8G", help='spark executor内存')
        # conf部分
        parser.add_argument('--conf', action='store', nargs='+', help='spark-submit --config会取代默认的')

        namespace, other_args = parser.parse_known_args(args=args)
        logging.info('spark conf and props:')
        logging.info(namespace)
        logging.info(other_args)

        props = {
            "--class": "com.mob.dataengine.utils.tools.Hive2Kafka",
            "--driver-memory": namespace.driver_memory,
            # "--driver-cores": namespace.driver_cores,
            "--executor-memory": namespace.executor_memory,
            "--executor-cores": namespace.executor_cores
        }
        conf = dict()
        if conf:
            for item in namespace.config:
                arr = item.split('=')
                conf[arr[0]] = arr[1]

        return props, conf

    def submit(self):
        logging.info(self.spark_args)
        self.spark2.submit(
            "dataengine-utils",
            args=self.spark_args,
            job_name='hive2kafka',
            job_id=str(uuid.uuid4()),
            props=self.props,
            conf=self.conf
        )
