package com.mob.dataengine.utils.hbase

import com.mob.dataengine.commons.enums.OSName
import com.mob.dataengine.utils.DateUtils
import com.mob.dataengine.utils.hbase.helper.TagsHFileGeneratorParams
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import scopt.OptionParser

object TagsHFileGeneratorBootStrap {

  def main(args: Array[String]): Unit = {
    /*
    val zk = "bd15-161-218,bd15-161-220,bd15-161-219"
    val numPartitions = 1024
    val hdfsPath = "/user/app360_test/tmp_menff/hfile_cols3"
    */
    val defaultParams: TagsHFileGeneratorParams = TagsHFileGeneratorParams()
    val projectName = s"TagsHFileGenerator[${DateUtils.currentDay()}]"
    val parser = new OptionParser[TagsHFileGeneratorParams](projectName) {
      head(s"$projectName")
      opt[String]('o', "os")
        .text("画像的种类")
        .required()
        .action((x, c) => c.copy(os = x))
      opt[String]('z', "zk")
        .text("zk地址")
        .required()
        .action((x, c) => c.copy(zk = x))
      opt[Int]('n', "partitions")
        .text("hfile的个数")
        .required()
        .action((x, c) => c.copy(numPartitions = x))
      opt[String]('h', "hdfsPath")
        .text("hdfs的路径")
        .action((x, c) => c.copy(hdfsPath = x))
      opt[String]('p', "prefix")
        .text(s"hbase列名的前缀")
        .action((x, c) => c.copy(prefix = x))
      opt[String]('r', "rowKey")
        .text(s"rowKey的字段")
        .action((x, c) => c.copy(rowKey = x))
      opt[String]('d', "day")
        .text("要导入的数据")
        .action((x, c) => c.copy(day = x))
      opt[Boolean]('f', "full")
        .text("是否全量表")
        .action((x, c) => c.copy(full = x))
    }

    // 数据源目前只是回溯的数据源

    parser.parse(args, defaultParams) match {
      case Some(params) =>
        val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
        val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
        println("delete path: " + params.hdfsPath)
        if (fs.exists(new Path(params.hdfsPath))) {
          fs.delete(new Path(params.hdfsPath), true)
        }

        val tagGen = (OSName.withName(params.os): @unchecked) match {
          case OSName.ANDROID =>
            AndroidTagsHFileGenerator(spark, params.day)
          case OSName.IOS => IosTagsHFileGenerator(spark, params.day)
        }
        val df = tagGen.transformData(params.full)
        tagGen.genHFile(spark, df, params.rowKey, params.zk, params.numPartitions,
          params.hdfsPath, params.prefix)
        spark.close();
      case _ => sys.exit(1)
    }
  }
}