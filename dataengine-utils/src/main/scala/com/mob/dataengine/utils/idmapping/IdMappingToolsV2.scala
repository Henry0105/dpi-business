package com.mob.dataengine.utils.idmapping

import com.mob.dataengine.commons.utils.Md5Helper
import com.mob.dataengine.rpc.RpcClient
import com.mob.dataengine.utils.DateUtils
import com.mob.dataengine.utils.idmapping.hive._
import org.apache.commons.lang.time.FastDateFormat
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.SparkSession
import scopt.OptionParser

case class Params(
  day: String = DateUtils.currentDay(),
  idTypes: String = "",
  action: String = "",
  rpcHost: Option[String] = None,
  rpcPort: Option[Int] = None,
  once: Option[Boolean] = Some(false),
  test: Boolean = false
) extends Serializable {
  def isRpc: Boolean = {
    rpcHost.isDefined && rpcPort.isDefined
  }
}

object IdMappingToolsV2 {
  def main(args: Array[String]): Unit = {
    val defaultParams: Params = Params()
    val projectName = s"IdMappingToolsV2[${DateUtils.currentDay()}]"

    val parser = new OptionParser[Params](projectName) {
      head(s"$projectName")
      opt[String]('a', "action")
        .text("hive|hbase")
        .required()
        .action((x, c) => c.copy(action = x))
      opt[String]('i', "id_types")
        .text("device|mac|imei|imei_14|idfa|phone|imsi|serialno")
        .required()
        .action((x, c) => c.copy(idTypes = x))
      opt[String]('d', "day")
        .text("day 例如:20180806 默认当天")
        .action((x, c) => c.copy(day = x))
      opt[Int]('p', "rpcPort")
        .text(s"thrift rpc port")
        .action((x, c) => c.copy(rpcPort = Some(x)))
      opt[Int]('r', "regions")
        .text(s"hbase regions")
        .action((x, c) => c.copy(rpcPort = Some(x)))
      opt[String]('h', "rpcHost")
        .text(s"thrift rpc host")
        .action((x, c) => c.copy(rpcHost = Some(x)))
      opt[Boolean]('o', "once")
        .text(s"once")
        .action((x, c) => c.copy(once = Some(x)))
      opt[Boolean]('t', "test")
        .text(s"test")
        .action((x, c) => c.copy(test = x))
    }
    parser.parse(args, defaultParams) match {
      case Some(params) =>
        val spark: SparkSession = SparkSession
          .builder()
          .appName(s"id-mapping-tools-v2")
          .config("spark.sql.orc.enableVectorizedReader", "true")
          .enableHiveSupport()
          .getOrCreate()
        if (params.isRpc) {
          RpcClient.send(params.rpcHost.get, params.rpcPort.get, s"1\u0001${spark.sparkContext.applicationId}")
        }
        // 注册用到的udf, udaf
        spark.udf.register("mergeOrUpdata", mergeOrUpdata _)
        spark.udf.register("get_latest_elements", getLatestElements _)
        spark.udf.register("trans_date", transDate _)
        spark.udf.register("merge_list", mergeLists _)
        spark.udf.register("get_max_elements", getMaxElements _)
        spark.udf.register("aggregateDevice", new AggregateIdTypeV2(200))
        spark.udf.register("get_tm", (arr: Seq[String], idx: Int) => {
          if (null == arr) {
            null
          } else {
            arr.map(e => e.split("\u0001")(idx))
          }
        })
        spark.udf.register("md5_array", md5Array _)
        spark.udf.register("collapse_array", collapseArray _)
        spark.udf.register("combine_incr", combineIncr _)
        spark.udf.register("combine_device_plat", combineDevicePlat _)

        params.action match {
          case "hive" =>
            params.idTypes.split(",").foreach {
              case "device" =>
                new DeviceMappingV2(spark, params.day, params.test).run()
              case idType@("idfa" | "imei" | "imsi" | "imei_14" | "serialno" | "oaid") =>
                val tools = new IdMappingV2(spark, params.day, idType, params.test)
                tools.prepare()
                tools.run()
              case idType@("phone" | "mac") =>
                val tools = new IdMapping2V2(spark, params.day, idType, params.test)
                tools.prepare()
                tools.run()
              case "external_id" =>
                val tools = new IdMappingExternalFull(spark, params.day, params.once, params.test)
                tools.prepare()
                tools.run()
              case idType =>
                spark.stop()
                throw new Exception(s"incorrect idType:[$idType]," +
                  s"only support[device,mac|imei|imei14|idfa|phone|imsi|serialno|oaid]")
            }
          case action =>
            throw new Exception(s"incorrect action:[$action],only support[hive|hbase|kafka]")
        }

        spark.stop()
      case _ => sys.exit(1)
    }
  }

  def getHbaseRegions(idType: String): Int = {
    idType match {
      case "device" => 512
      case "mac" => 256
      case "imei" => 128
      case "imei_14" => 128
      case "idfa" => 64
      case "phone" => 32
      case _ => 64
    }
  }

  // 根据arrLtm取最新的limit个arr,arrTm,arrLtm的元素
  def getLatestElements(arr: Seq[String], arrTm: Seq[String], arrLtm: Seq[String], limit: Int):
  (Seq[String], Seq[String], Seq[String]) = {
    if (null == arr) {
      (null, null, null)
    } else {
      if (arr.length <= limit) {
        (arr, arrTm, arrLtm)
      } else {
        val tmp = arr.zip(arrTm).zip(arrLtm).map(x => (x._1._1, x._1._2, x._2))
          .sortWith((a, b) => a._3 > b._3).slice(0, limit)
        (tmp.map(_._1), tmp.map(_._2), tmp.map(_._3))
      }
    }
  }

  def transDate(dateSeq: Seq[String]): Seq[String] = {
    val fdf = FastDateFormat.getInstance("yyyyMMdd")
    if (dateSeq != null) dateSeq.map(dateItem =>
      org.apache.commons.lang3.time.DateUtils.parseDate(dateItem, "yyyyMMdd"))
      .map(item => (item.getTime()/1000).toString)
    else Seq()
  }


  // 将日新增的列表融入全量表中
  def mergeLists(fullArr: Seq[String], incrArr: Seq[String]): Seq[String] = {
    if (null == incrArr) {
      fullArr
    } else if (null == fullArr) {
      incrArr
    } else {
      fullArr ++ (incrArr diff fullArr)
    }
  }
  // 增量数据 full join 后 需要判断数据是更新还是新增
  def mergeOrUpdata(fullArrKye: Seq[String], fullArrValue: Seq[String],
                    incrArrKye: Seq[String], incrArrValue: Seq[String]): Seq[String] = {
    if (null == incrArrValue) {
      fullArrValue
    } else if (null == fullArrValue) {
      incrArrValue
    } else {
      if ((incrArrKye diff fullArrKye).nonEmpty) {
        var value = fullArrValue.toArray
        for (i <- incrArrKye.indices) {
          if (fullArrKye.contains(incrArrKye(i))) {
            val index = fullArrKye.indexOf(incrArrKye(i))
            value(index) = incrArrValue(i)
          } else {
            value = value :+ incrArrValue(i)
          }
        }
        value
      } else {
        val fullArr = fullArrValue.toArray
        for (i <- incrArrKye.indices) {
          if (fullArrKye.contains(incrArrKye(i))) {
            val index = fullArrKye.indexOf(incrArrKye(i))
            fullArr(index) = incrArrValue(i)
          }
        }
        fullArr
      }
    }
  }

  // 取arr里limit个元素
  def getMaxElements(arr: Seq[String], limit: Int): Seq[String] = {
    if (null == arr) {
      null
    } else {
      if (arr.length <= limit) {
        arr
      } else {
        arr.slice(0, limit)
      }
    }
  }

  def md5Array(arr: Seq[String]): Seq[String] = {
    if (null == arr || arr.isEmpty) {
      null
    } else {
      arr.map(Md5Helper.entryMD5_32)
    }
  }

  // 将明文聚合的数据和md5聚合的数据进行合并
  // 1. 如果明文中的数据已经全包含了md5的数据,则将md5的值置为空
  // 2. 如果明文中的数据没有包含md5的数据, 则将对应的明文置为空
  def collapseArray(s1: Seq[String], s1Tm: Seq[String], s1Ltm: Seq[String], s2Md5: Seq[String],
    s2Md5Tm: Seq[String]): (Seq[String], Seq[String], Seq[String], Seq[String]) = {
    if (null == s1 || s1.isEmpty) {
      (null, s2Md5, s2Md5Tm, s2Md5Tm)
    } else if (null == s2Md5 || s2Md5.isEmpty) { // 没有外部数据的时候,不生成MD5
      (s1, null, s1Tm, s1Ltm)
    } else {
      val s1Md5 = md5Array(s1)
      val s1Md5Set = s1Md5.toSet

      val (_s2, _s2Md5, _s2Tm) = s2Md5.zip(s2Md5Tm).filterNot{ case (idMd5, tm) => s1Md5Set.contains(idMd5)}
        .map{ case (idMd5, tm) => ("", idMd5, tm)}.unzip3

      (s1 ++ _s2, s1Md5 ++ _s2Md5, s1Tm ++ _s2Tm, s1Ltm ++ _s2Tm)
    }
  }

  // 将明文和密文合并起来
  def getFullMd5(s: Seq[String], sMd5: Seq[String]): Set[String] = {
    if (null == sMd5 || sMd5.isEmpty) {
      md5Array(s.filter(_.nonEmpty)).toSet
    } else {
      (md5Array(s.filter(_.nonEmpty)) ++ sMd5).toSet
    }
  }

  // 将明文/密文/tm/ltm合并成一个map, 以md5为主键
  def buildDataMap(seq: Seq[String], md5Seq: Seq[String], tmSeq: Seq[String],
    ltmSeq: Seq[String]): Map[String, (String, String, String)] = {
    if (null == md5Seq || md5Seq.isEmpty) {  // 只有明文,没有密文
      seq.indices.map{ i =>
        (Md5Helper.entryMD5_32(seq(i)), (seq(i), tmSeq(i), ltmSeq(i)))
      }.toMap
    } else { // 有密文, 此时密文肯定和明文长度相同
      md5Seq.indices.map{ i =>
        (md5Seq(i), (seq(i), tmSeq(i), ltmSeq(i)))
      }.toMap
    }
  }

  // 将map进行清理, 如果所有的明文都有对应的密文, 则去掉所有的密文
  // 如果只有部分明文有对应的密文,则都保留
  // 返回: 明文, 密文, tm, ltm
  def cleanAndTransformMap(m: Map[String, (String, String, String)]): (Seq[String],
    Seq[String], Seq[String], Seq[String]) = {
    var shouldClean = true
    val res = m.foldLeft((Seq.empty[String], Seq.empty[String], Seq.empty[String],
      Seq.empty[String])) { case (acc, element) =>
        val md5 = element._1
        val (s, tm, ltm) = element._2
        if (StringUtils.isBlank(s)) { // 存在只有密文, 没有明文的情况
          shouldClean = false
        }
      (s +: acc._1, md5 +: acc._2, tm +: acc._3, ltm +: acc._4)
    }
    if (shouldClean) {
      (res._1, null, res._3, res._4)
    } else {
      res
    }
  }

  // 因为 math.max 不支持string类型
  def maxString(s1: String, s2: String): String = {
    if (s1 > s2) {
      s1
    } else {
      s2
    }
  }

  // 因为 math.min 不支持string类型
  def minString(s1: String, s2: String): String = {
    if (s1 > s2) {
      s2
    } else {
      s1
    }
  }

  // 全量表合并增量表的数据
  // 实现的功能类似于上面的collapseArray
  // s1: 全量, s2: 增量
  def combineIncr(s1: Seq[String], s1Md5: Seq[String], s1Tm: Seq[String], s1Ltm: Seq[String],
    s2: Seq[String], s2Md5: Seq[String], s2Tm: Seq[String], s2Ltm: Seq[String]): (Seq[String],
    Seq[String], Seq[String], Seq[String]) = {

    if (null == s1 || s1.isEmpty) {  // 在增量表和全量表里面s1, s1md5的长度是相同的, 所以只需要判断1个
      (s2, s2Md5, s2Tm, s2Ltm)
    } else if (null == s2 || s2.isEmpty) {
      (s1, s1Md5, s1Tm, s1Ltm)
    } else {
      val s1Map = buildDataMap(s1, s1Md5, s1Tm, s1Ltm)
      val s2Map = buildDataMap(s2, s2Md5, s2Tm, s2Ltm)

      val updatedS1Map = s1Map.map{ case (md5, (s, tm, ltm)) =>
        if (s2Map.contains(md5)) {  // 此时需要更新明文, tm, ltm
          (md5, (if (s.length > 0) s else s2Map(md5)._1, minString(tm, s2Map(md5)._2), maxString(ltm, s2Map(md5)._3)))
        } else {
          (md5, (s, tm, ltm))
        }
      }

      val diffMap = s2Map.filter{ case (k, _) => !s1Map.contains(k)}

      cleanAndTransformMap((updatedS1Map ++ diffMap).toSeq.sortWith((a, b) => a._2._3 > b._2._3).slice(0, 200).toMap)
    }
  }

  // mac_mapping/phone_mapping中的device_plat字段需要合并下
  // d1表示全量, d2表示增量
  def combineDevicePlat(d1: Seq[String], dp1: Seq[String], d2: Seq[String], dp2: Seq[String]): Seq[String] = {
    if (null == d1 || d1.isEmpty) {  // 之前没有device
      dp2
    } else if (null == d2 || d2.isEmpty) {
      dp1
    } else {
      val deviceDiff = d2.diff(d1).toSet
      val newPlats = d2.zip(dp2).filter { case (d, _) => deviceDiff.contains(d) }.map(_._2)
      dp1 ++ newPlats
    }
  }
}

/*
 *
 * --conf "spark.memory.offHeap.enabled=true"        \
 * --conf "spark.memory.offHeap.size=1024M"        \
 * --conf "spark.sql.shuffle.partitions=1024"       \
 * --conf "spark.memory.storageFraction=0.1"        \
 *
 *
 * spark2-submit --executor-memory 12G        \
 *   --master yarn        \
 *   --executor-cores 4        \
 *   --name id_mapping_tool-20180810        \
 *   --deploy-mode cluster        \
 *   --class com.mob.dataengine.utils.idmapping.IdMappingTools        \
 *   --driver-memory 4G        \
 *   --conf "spark.yarn.appMasterEnv.JAVA_HOME=/opt/jdk1.8.0_45"        \
 *   --conf "spark.speculation.quantile=0.95"        \
 *   --conf "spark.executorEnv.JAVA_HOME=/opt/jdk1.8.0_45"        \
 *   --conf "spark.executor.extraJavaOptions=-XX:+UseG1GC -Dlog4j.configuration=file:log4j.properties" \
 *   --conf "spark.driver.extraJavaOptions=-XX:+UseG1GC -Dlog4j.configuration=file:log4j.properties"        \
 *   --conf "spark.speculation=true"        \
 *   --conf "spark.shuffle.service.enabled=true"        \
 *   --conf "spark.network.timeout=300s"        \
 *   --conf "spark.reducer.maxSizeInFlight=8m"        \
 *   --conf "spark.reducer.maxReqsInFlight=1"        \
 *   --files /home/dataengine/releases/midengine/20180817-184508/distribution/conf/log4j.properties              \
 *   /home/dataengine/releases/midengine/20180817-184508
 *   /distribution/lib/dataengine-utils-v0.7.2.1-jar-with-dependencies.jar   \
 *   -i device -d 20180818 -a hive
 *
 *
 * spark2-submit --executor-memory 12G  \
 *  --master yarn \
 *  --executor-cores 4  \
 *  --name id_mapping_tool-20180818-3fcb6c3f-1751-4b8f-a019-7d5acbd53fda  \
 *  --deploy-mode cluster \
 *  --class com.mob.dataengine.utils.idmapping.IdMappingTools \
 *  --driver-memory 8G  \
 *  --conf "spark.yarn.appMasterEnv.JAVA_HOME=/opt/jdk1.8.0_45" \
 *  --conf "spark.speculation.quantile=0.98"  \
 *  --conf "spark.executorEnv.JAVA_HOME=/opt/jdk1.8.0_45" \
 *  --conf "spark.executor.extraJavaOptions=-XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35" \
 *  --conf "spark.speculation=true" \
 *  --conf "spark.shuffle.service.enabled=true" \
 *  --conf "spark.sql.shuffle.partitions=1024"  \
 *          --files /home/dataengine/releases/midengine/20180817-184508/distribution/conf/log4j.properties \
 *          /home/dataengine/releases/midengine/20180817-184508
 *          /distribution/lib/dataengine-utils-v0.7.2.1-jar-with-dependencies.jar  \
 *           -i device -d 20180818 -a hive
 *
 * spark2-shell --executor-memory 12G        \
 *   --executor-cores 4        \
 *   --driver-memory 4G        \
 *   --conf "spark.yarn.appMasterEnv.JAVA_HOME=/opt/jdk1.8.0_45"        \
 *   --conf "spark.speculation.quantile=0.95"        \
 *   --conf "spark.executorEnv.JAVA_HOME=/opt/jdk1.8.0_45"        \
 *   --conf "spark.speculation=true"        \
 *   --conf "spark.shuffle.service.enabled=true"        \
 *   --conf "spark.network.timeout=300s"
 */
