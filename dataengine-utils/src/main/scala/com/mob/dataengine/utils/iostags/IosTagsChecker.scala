package com.mob.dataengine.utils.iostags

import com.mob.dataengine.commons.traits.Logging
import com.mob.dataengine.utils.iostags.beans.{Param, QueryUnitContext}
import com.mob.dataengine.utils.iostags.handle.QueryUnitFactory
import com.mob.dataengine.utils.iostags.helper.{MetaDataHelper, TablePartitionsManager}
import org.apache.spark.sql.SparkSession
import scopt.OptionParser

/**
 * @author xlmeng
 */
object IosTagsChecker {

  def main(args: Array[String]): Unit = {
    val defaultParam: Param = Param()
    val projectName = "TagsChecker"
    val parser = new OptionParser[Param](projectName) {
      head(s"$projectName")
      opt[String]('d', "day")
        .required()
        .action((x, c) => c.copy(day = x))
    }

    parser.parse(args, defaultParam) match {
      case Some(p) =>
        println(s"参数为:$p")
        val spark = SparkSession
          .builder()
          .appName(this.getClass.getSimpleName + p.day)
          .enableHiveSupport()
          .getOrCreate()

        IosTagsChecker(spark, p).run()
        spark.close()
      case _ =>
        println(s"参数有误，参数为: ${args.mkString(",")}")
        sys.exit(1)
    }
  }
}

case class IosTagsChecker(@transient spark: SparkSession, p: Param) extends Logging {

  def run(): Unit = {

    // 取出需要检查的表和频率信息
    val tbManager = TablePartitionsManager(spark)

    val profiles = MetaDataHelper.getComputedProfiles(spark, p.day)
    profiles.groupBy(_.fullTableName).foreach { case (_, ps) =>
      println(s"${ps.head.fullTableName}=>${ps.length}")
    }
    val cxt = QueryUnitContext(spark, p.day, tbManager)
    val queryUnits = QueryUnitFactory.createQueryUnit(cxt, profiles)
    println(queryUnits.map(_.msg).mkString("\n"))
    val checkRes = queryUnits.map(_.check())

    if (!checkRes.forall(_._2)) {
      val errorSet = checkRes.filterNot(_._2).groupBy(e => s"${e._1}/${e._3}").keySet
      errorSet.foreach(errMsg => logger.error(s"Error: 有表分区未生成 => $errMsg"))

      val errorPartSet = checkRes.filterNot(_._2).groupBy(_._1).keySet
      errorPartSet.foreach(errPart => logger.error(s"Error: 表未生成 => $errPart"))
      spark.close()
      sys.exit(1)
    }

  }

}

