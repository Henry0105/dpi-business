package com.mob.dataengine.engine.core.jobsparam

import java.util.regex.Pattern

import com.mob.dataengine.commons.{BaseParam2, DeviceSrcReader, JobCommon}
import com.mob.dataengine.commons.pojo.MatchInfoV2
import com.mob.dataengine.commons.traits.{PartitionTrait, TableTrait, UDFCollections}
import com.mob.dataengine.commons.utils.PropUtils
import com.mob.dataengine.rpc.RpcClient
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, concat_ws, map}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql._
import org.apache.spark.storage.StorageLevel
import org.json4s.DefaultFormats
import com.mob.dataengine.commons.annotation.code.{author, createTime, explanation, sourceTable, targetTable}
import com.mob.dataengine.commons.service.{DataHubService, DataHubServiceImpl}
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

import scala.collection.mutable

/**
 * 目前中间件的任务基本为以下 :
 * 1.build input DF 创建输入的dataframe
 * 2.transform DF 对dataframe进行transform操作
 * 3.persist to hive 将处理好的数据持久化到hive
 * 4.transform write HDFS DF 对写入hdfs的df做transform操作
 * 5.write to HDFS 将处理好的数据持久化到HDFS 导出给用户
 * 6.RPC 通过rpc发送任务内的具体指标
 * 因此，封装此基类。1，3，4，5，6步骤设置了默认方法，如果不满足任务需求可自行重写
 * 第 2 步骤为transform操作，需要根据具体任务计算逻辑自行重写
 * 第 4 步骤的transform操作，目前需求是为了判断是否保留种子数据，为了方便后续修改需求，因此单独封装
 * 具体适用实例见数据清洗任务（DalaCleaning）
 * */

@author("jiandd")
@createTime("2020-01-06")
case class JobContext2[T <: BaseParam2](spark: SparkSession, jobCommon: JobCommon, param: T) extends TableTrait {
  var matchInfo: MatchInfoV2 = MatchInfoV2(uuid = param.output.uuid)

  lazy val hubService: DataHubService = DataHubServiceImpl(spark)

  override def toString: String = {
    s"$jobCommon, $param"
  }

  def matchInfoJson(): String = {
    val json =
      ("job_id" -> jobCommon.jobId) ~ ("uuid" -> param.output.uuid) ~ ("id_cnt" -> matchInfo.idCnt) ~
        ("match_cnt" -> matchInfo.matchCnt) ~ ("out_cnt" -> matchInfo.outCnt.toJson())
    compact(render(json).merge(render(map2jvalue(matchInfo.m.toMap[String, Long]))))
  }

  private val status: mutable.Map[String, Any] = mutable.Map[String, Any]()

  def getStatus[V](key: String): V = {
    this.status(key).asInstanceOf[V]
  }

  def putStatus(key: String, value: Any): Unit = {
    this.status.put(key, value)
  }
}

abstract class BaseJob2[T <: BaseParam2](implicit mf: Manifest[Seq[T]])
  extends UDFCollections with Serializable with PartitionTrait {

  @transient implicit val formats: DefaultFormats.type = DefaultFormats


  @explanation("选择是否将输入的数据cache，必须实现赋值")
  def cacheInput: Boolean

  /**
   * @param df transform dataframe
   * @param ctx 任务上下文
   * */
  @explanation("transform 方法，不包含默认方法，需要根据任务的具体逻辑 进行实现")
  def transformData(df: DataFrame, ctx: JobContext2[T]): DataFrame


  /**
   * @param ctx 任务执行上下文
   * */
  @explanation("创建 输入数据的 dataframe ， source dataframe")
  @sourceTable("rp_dataengine.data_opt_cache_new,rp_dataengine.data_opt_cache")
  def buildInputDataFrame(ctx: JobContext2[T]): DataFrame = {
    import ctx.spark.implicits._
    val rawRDD = ctx.param.inputs.map(input => DeviceSrcReader.toRDD2(ctx.spark, input)).reduce(_ union _)
    println(rawRDD.isEmpty())
    if (ctx.param.sep.isDefined) {
      require(ctx.param.idx.nonEmpty)
      require(ctx.param.header > 0)
      require(ctx.param.headers.nonEmpty)
      require(ctx.param.headers.get.size >= ctx.param.idx.get)
      val tbSchema: StructType =
        StructType(ctx.param.headers.get.map(StructField(_, StringType, nullable = true)))
      ctx.spark.createDataFrame(rawRDD
        // 去掉headers行
        .filter(s => !s.equals(ctx.param.headers.get.map(_.substring(3))
          .mkString(ctx.param.sep.get)))
        .map{ r =>
          val arr: Seq[String] = r.split(
            Pattern.quote(ctx.param.sep.get),
            ctx.param.headers.get.size
          )
          Row.fromSeq(arr)
        }, tbSchema).toDF(ctx.param.headers.get: _*)
    } else {
      rawRDD.toDF(ctx.param.origJoinField)
    }
  }


  /**
   * @param df 持久化到hive的dataframe
   * @param ctx 任务上下文
   * */
  @explanation("计算后的数据持久化到hive sink hive")
  @targetTable("rp_dataengine.data_opt_cache_new")
  def persist2Hive(df: DataFrame, ctx: JobContext2[T]): Unit = {
    val cleanMapUDF: UserDefinedFunction = org.apache.spark.sql.functions.udf(clean_map _)
    var tmp = df
    // 做出match_ids列
    val outCols = createMatchIdsCol(ctx)
    tmp = tmp.withColumn("match_ids", cleanMapUDF(map(outCols: _*)))
    // 做出id列
    tmp = tmp.withColumn("id", col(ctx.param.origJoinField))
    // 做出data列, 将输入数据中的其他列方式data中,并拼接
    val dataCol = if (ctx.param.header > 0) {
      val otherIds = ctx.param.headers.get
      concat_ws(ctx.param.sep.get, otherIds.map(col): _*)
    } else {
      tmp("id")
    }
    tmp = tmp.withColumn("data", dataCol)
    tmp.createOrReplaceTempView("tmp")
    ctx.sql(
      s"""
         |insert overwrite table ${PropUtils.HIVE_TABLE_DATA_OPT_CACHE_NEW}
         |partition(uuid='${ctx.param.output.uuid}', day='${ctx.jobCommon.day}')
         |select id, match_ids, ${ctx.param.inputIdType} as id_type,
         | ${ctx.param.inputEncrypt.encryptType} as encrypt_type, data
         |from tmp
        """.stripMargin)
  }


  /**
   * @param ctx 任务上下文
   * */
  @explanation("创建 match_ids 列，根据任务需求，实现此方法创建match_ids")
  def createMatchIdsCol(ctx: JobContext2[T]): Seq[Column]


  /**
   * @param df 输入dataframe
   * @param ctx 任务上下文
   * */
  @explanation("数据持久化到 HDFS，如果数据需要导出的时候需要将数据持久化到 HDFS， sink HDFS")
  def write2HDFS(df: DataFrame, ctx: JobContext2[T]): Unit = {
    var resultDF = df
    if (!ctx.param.output.hdfsOutput.isEmpty) {
      if (ctx.param.output.limit.getOrElse(-1) != -1 ) {
        resultDF = resultDF.limit(ctx.param.output.limit.get)
      }
      resultDF.repartition(1).write.mode(SaveMode.Overwrite)
        .option("emptyValue", null)
        .option("sep", "\t")
        .csv(ctx.param.output.hdfsOutput)
    }
  }


  /**
   * @param df 输入DF
   * @param ctx 任务上下文
   * */
  @explanation("持久化到HDFS前需要判断 是否需要导出种子包 keepSeed：1 表示导出，0 表示不导出")
  def transformSeedDF(df: DataFrame, ctx: JobContext2[T]): DataFrame = {
    val dataFrame = if (ctx.param.output.keepSeed != 1) {
      df.select(s"${ctx.param.fieldName}")
    } else {
      df.select("data", s"${ctx.param.fieldName}")
    }
    dataFrame
  }


  /**
   * @param ctx 任务上下文
   * */
  @explanation("通过RPC发送任务内的具体数据，如输出uuid，idCnt，matchCnt等信息")
  def sendRPC(ctx: JobContext2[T]): Unit = {
    if (ctx.jobCommon.hasRPC()) {

      RpcClient.send(ctx.jobCommon.rpcHost, ctx.jobCommon.rpcPort,
        s"2\u0001${ctx.param.output.uuid}\u0002${ctx.matchInfoJson()}")
    }
  }


  @explanation("任务比较通用的执行逻辑，如后续需求此执行逻辑不满足，可进行重写")
  def run(ctx: JobContext2[T]): Unit = {
    implicit val spark: SparkSession = ctx.spark
    println(ctx)
    val buildDF = buildInputDataFrame(ctx)
    if (cacheInput) buildDF.cache()
    ctx.matchInfo.idCnt = buildDF.count()
    var transDF = transformData(buildDF, ctx)
    transDF.persist(StorageLevel.MEMORY_AND_DISK)
    ctx.matchInfo.setMatchCnt(transDF.count())

    transDF = transDF.repartition(getCoalesceNum(ctx.matchInfo.matchCnt, 100000))
    persist2Hive(transDF, ctx)
    val seedDF = transformSeedDF(transDF, ctx)
    write2HDFS(seedDF, ctx)
    sendRPC(ctx)
  }

  // 代码中不要使用该变量, 仅用在单元测试
  var jobContext: JobContext2[T] = null

  def main(args: Array[String]): Unit = {
    val arg = args(0)
    println(s"arg ==> $arg")

    val PARAMS_KEY = "params"
    val jobCommon = JobParamTransForm.humpConversion(arg)
      .extract[JobCommon]
    val params = (JobParamTransForm.humpConversion(arg) \ PARAMS_KEY)
      .extract[Seq[T]]

    lazy val spark = SparkSession
      .builder()
      .appName(jobCommon.jobId)
      .enableHiveSupport()
      .getOrCreate()
    if (jobCommon.hasRPC()) {
      RpcClient.send(jobCommon.rpcHost, jobCommon.rpcPort, s"1\u0001${spark.sparkContext.applicationId}")
    }


    params.foreach{ param =>
      jobContext = JobContext2(spark, jobCommon, param)
      run(jobContext)
    }
  }
}
