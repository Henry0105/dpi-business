package com.mob.dataengine.utils.pidXid

import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.mob.dataengine.commons.utils.PropUtils

object PidXid {
    @transient private[this] val logger = Logger.getLogger(this.getClass)
    def main(args: Array[String]): Unit = {

        val day = args(0)
        val isFull = args(1)  // 是否跑全量
        val hdfs_path = args(2) // hdfs根目录
        val yesterday = args(3)
        val Spark = SparkSession.builder
            .enableHiveSupport()
            .config("hive.exec.dynamici.partition", true)
            .config("hive.exec.dynamic.partition.mode", "nonstrict")
            .getOrCreate()

        val hdfs_file_path = hdfs_path + "/" + day + ".txt"
        logger.info("hdfs_file_path: " + hdfs_file_path)

        import Spark.implicits._
        val phoneXidDF: DataFrame = Spark.read.textFile(hdfs_file_path).map(phoneXidString => {
            val phoneXid = phoneXidString.split("\t")
            (phoneXid(0), phoneXid(1))
        }).toDF("phone", "xid")
        phoneXidDF.createTempView("phone_xid_table")

        Spark.sql("add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/pid_encrypt.jar")
        Spark.sql("create temporary function pid_encrypt as 'com.mob.udf.PidEncrypt'")

        val writeTable = PropUtils.HIVE_TABLE_DM_DATAENGINE_PID_XID

        val write_sql = if (isFull.toBoolean) {
            s"""  insert overwrite table $writeTable partition(version=$day)
                       |select pid_encrypt(phone) pid, xid from phone_xid_table """.stripMargin
        } else {
            s"""  insert overwrite table $writeTable partition(version=$day)
                       |select pid_encrypt(phone) pid, xid from phone_xid_table
                       |union all
                       |select pid, xid from $writeTable where version=$yesterday """.stripMargin
        }

        logger.info("write_sql: " + write_sql)
        Spark.sql(write_sql)

    }
}
