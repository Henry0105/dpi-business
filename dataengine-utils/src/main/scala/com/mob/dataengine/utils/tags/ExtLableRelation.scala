package com.mob.dataengine.utils.tags.profile

import java.time.LocalDate
import java.time.format.DateTimeFormatter

import com.mob.dataengine.commons.Encrypt
import com.mob.dataengine.commons.enums.EncryptType._
import com.mob.dataengine.commons.enums.ExtValueType
import com.mob.dataengine.commons.profile.MetadataUtils
import com.mob.dataengine.commons.utils.PropUtils._
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{StructField, _}
import scopt.OptionParser

/**
 * @author : xlmeng
 * @date : 2020/4/29
 */
case class Params(day: String = "") {
  override def toString: String = s"""day=$day"""
}

object ExtLableRelation {
  val NONID: Int = NONENCRYPT.id
  val MD5ID: Int = MD5_32ENCRYPT.id
  val SHA256ID: Int = SHA256ENCRYPT.id

  def encryptType(`type`: String): Int = {
    val t = ExtValueType.withName(`type`)
    ExtValueType.determineEncryptType(t).id
  }

  /**
   * 个推所允许的标签集合:  imeimd5, macmd5, imsimd5, phonemd5, idfa
   * 极光所允许的标签集合:  phone, phonemd5, phonesha256, mac, imsi, imei14, imei15, imei
   * 运营商所允许的标签集合: phone
   */
  def transValue(value: String, id: Int): Map[Int, String] = {
    id match {
      case NONID =>
        Map(
          NONID -> value,
          MD5ID -> Encrypt(MD5ID).compute(value),
          SHA256ID -> Encrypt(SHA256ID).compute(value)
        )
      case MD5ID =>
        Map(MD5ID -> value)
      case SHA256ID =>
        Map(SHA256ID -> value)
    }
  }

  def transLabel(con: Seq[Map[String, String]]): Map[String, String] = {
    con.filter(map => (map.keySet intersect Set("mobid", "mobvalue")).size == 2)
      .map(map => (map("mobid"), map("mobvalue"))).toMap
  }

  class ValueConcat extends UserDefinedAggregateFunction {
    override def inputSchema: StructType = StructType(StructField("value", MapType(IntegerType, StringType)) :: Nil)

    override def bufferSchema: StructType = StructType(StructField("value", MapType(IntegerType, StringType)) :: Nil)

    override def dataType: DataType = MapType(IntegerType, StringType)

    override def deterministic: Boolean = true

    override def initialize(buffer: MutableAggregationBuffer): Unit = Map.empty[Int, String]

    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      this.merge(buffer, input)
    }

    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      val map1 = buffer1.getAs[Map[Int, String]](0)
      val map2 = buffer2.getAs[Map[Int, String]](0)
      val newMap = (map1 != null && map1.nonEmpty, map2 != null && map2.nonEmpty) match {
        case (true, true) => map1 ++ map2
        case (true, false) => map1
        case (false, true) => map2
      }
      buffer1.update(0, newMap)
    }

    override def evaluate(buffer: Row): Any = buffer.getAs[Map[Int, String]](0)
  }

  /** label字段合并,取最新的label(按flag升序) */
  class LabelConcat extends UserDefinedAggregateFunction {
    override def inputSchema: StructType =
      StructType(StructField("label", MapType(StringType, StringType)) ::
        StructField("flag", IntegerType) :: Nil)

    override def bufferSchema: StructType =
      StructType(StructField("label", MapType(StringType, StringType)) ::
        StructField("flag", IntegerType) :: Nil)

    override def dataType: DataType = MapType(StringType, StringType)

    override def deterministic: Boolean = true

    override def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer(0) = Map.empty[String, String]
      buffer(1) = Int.MaxValue
    }

    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      merge(buffer, input)
    }

    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      val map1 = buffer1.getAs[Map[String, String]](0)
      val flag1 = buffer1.getAs[Int](1)
      val map2 = buffer2.getAs[Map[String, String]](0)
      val flag2 = buffer2.getAs[Int](1)
      val (rMap, rFlag) = if (flag1 < flag2) (map2 ++ map1, flag1) else (map1 ++ map2, flag2)
      buffer1.update(0, rMap)
      buffer1.update(1, rFlag)
    }

    override def evaluate(buffer: Row): Any = buffer.getAs[Map[String, String]](0)
  }

  def main(args: Array[String]): Unit = {
    val defaultParams: Params = Params()
    val projectName = s"ExtLableRelation[${LocalDate.now}]"

    val parser = new OptionParser[Params](projectName) {
      head(s"$projectName")
      opt[String]('d', "day")
        .text("任务日期")
        .required()
        .action((x, c) => c.copy(day = x))
    }

    parser.parse(args, defaultParams) match {
      case Some(p) =>
        val spark: SparkSession = SparkSession.builder()
          .appName(this.getClass.getSimpleName)
          .config("hive.exec.dynamic.partition", "true")
          .config("hive.exec.dynamic.partition.mode", "nonstrict")
          .config("spark.sql.hive.convertMetastoreOrc", "false")
          .enableHiveSupport()
          .getOrCreate()
        println(p)
        new ExtLableRelation(spark, p).run()
        spark.close()
      case _ => sys.exit(1)
    }
  }
}


/**
 * 需要将相同的value做聚合，方法:将明文加密去撞密文，相同聚合。
 * 目前加密格式有 明文(non),md5,sha256
 * 产生
 * incr:non,md5,sha256 3种组合
 * full:non(只有明文),md5,sha256,non&md5,non&sha256,non&md5&sha25 6种组合
 * pre:   incr格式化成full表数据结构,两者union all
 * step1: non，md5，non&md5，non&md5&sha256 这四种一起处理,分成只有md5加密的(onlyMd5DF)和包含明文的部分(step1DF)。
 * step2: step1DF和sha256，non&sha256 一起处理,union all上onlyMd5DF得到最终结果
 */
class ExtLableRelation(@transient spark: SparkSession, p: Params) extends Serializable {

  import ExtLableRelation._
  import spark.sql
  import spark.implicits._
  import spark.{sparkContext => sc}
  import org.apache.spark.sql.functions._

  // udaf register
  val label_concat = new LabelConcat
  val value_concat = new ValueConcat
  // schema
  val header: List[Column] = List("value", "type", "label", "channel").map(col)
  val tmpHeader: List[Column] = List("flag", "id").map(col)

  def run(): Unit = {
    val fmt = DateTimeFormatter.ofPattern("yyyyMMdd")
    val yesterday = LocalDate.parse(p.day, fmt).plusDays(-1).format(fmt)
    val viewName = "out_view"

    val idBC = sc.broadcast(MetadataUtils.findModId2ProfileID().toMap)
    def labelModId2ProfileID(m: Map[String, String]): Map[String, String] = {
      val vIdMapping = idBC.value
      m.map { case (k, v) => (vIdMapping(k), v) }
    }
    // udf register
    val encrypt_type = udf(encryptType(_: String))
    val type2value = udf(transValue(_: String, _: Int))
    val trans_label = udf(transLabel(_: Seq[Map[String, String]]))
    val mobid2profileid = udf(labelModId2ProfileID(_: Map[String, String]))

    // flag为标记位(1为incr,2为full)
    val incrDF = sql(
      s"""
         |select   1 as flag, value, type, data as label, channel
         |from    $HIVE_TABLE_LABEL_RELATION
         |where   day = '${p.day}'
         |""".stripMargin)

    val fullDF = sql(
      s"""
         |select  2 as flag, map_keys(value) as id
         |      , value, type, label, channel
         |from    $HIVE_TABLE_EXT_LABEL_RELATION_FULL
         |where   day = '$yesterday'
         |""".stripMargin)

    val unionDF = incrDF
      .withColumn("id", encrypt_type(lower($"type")))
      .withColumn("type", array($"type"))
      .withColumn("value", type2value($"value", $"id"))
      .withColumn("id", array($"id"))
      .withColumn("label", trans_label($"label"))
      .where("cardinality(label) <> 0")
      .withColumn("label", mobid2profileid($"label"))
      .select(tmpHeader ::: header: _*)
      .union(fullDF).cache()

    val condi1 = $"id" <=> array(lit(NONID)) || array_contains($"id", MD5ID)
    val nonMd5DF = unionDF.where(condi1)
    val sha256DF = unionDF.where(!condi1)

    val step1DF = aggProcess(nonMd5DF.withColumn("groupCol", element_at($"value", MD5ID))).cache()

    val condi2 = $"id" <=> array(lit(MD5ID))
    val onlyMd5DF = step1DF.where(condi2)

    val step2DF = aggProcess(step1DF
      .where(!condi2)
      .union(sha256DF)
      .withColumn("groupCol", element_at($"value", SHA256ID)))

    step2DF.union(onlyMd5DF).drop(tmpHeader.map(_.toString): _*).createOrReplaceTempView(viewName)

    sql(
      s"""
         |insert overwrite table $HIVE_TABLE_EXT_LABEL_RELATION_FULL partition ( day = ${p.day}, channel )
         |select ${header.mkString(",")}
         |from $viewName
         |""".stripMargin)

    unionDF.unpersist()
    step1DF.unpersist()
  }

  def aggProcess(df: DataFrame): DataFrame = {
    df.groupBy($"groupCol", $"channel")
      .agg(value_concat($"value").as("value"),
        array_distinct(flatten(collect_list($"type"))).as("type"),
        label_concat($"label", $"flag").as("label"),
        min($"flag").as("flag"),
        array_distinct(flatten(collect_list($"id"))).as("id"))
      .select(tmpHeader ::: header: _*)
  }
}
