package com.mob.dataengine.utils.apppkg2vec

import org.ansj.recognition.impl.FilterRecognition
import org.ansj.splitWord.analysis.DicAnalysis
import org.apache.spark.sql.SparkSession

import scala.io.Source

// 中文分词UDF
object WordSplitterUDF extends Serializable {

  val dicName = "stopWords.dic"
  val recognition: FilterRecognition = using(WordSplitterUDF.getClass.getClassLoader.getResourceAsStream(dicName)) {
    in =>
      val stopWords = Source.fromInputStream(in).getLines().toSeq
      println(s"load stopWords.dic, example => [${stopWords.take(10).mkString("|")}]")
      val _recognition = new FilterRecognition()
      _recognition.insertStopWord(stopWords ++
        Seq("w", null, "u", "xu", "xx", "e", "p", "ude1", "vshi", "vyou", "m"
        ): _*)
      _recognition
  }

  def parse(desc: String): Seq[String] = {
    import scala.collection.JavaConverters._
    DicAnalysis.parse(desc).recognition(recognition).getTerms.asScala.map(_.getName)
  }

  // scalastyle:off
  def using[Closable <: { def close(): Unit }, T](conn: Closable)(f: Closable => T): T =
    try { f(conn) } finally { conn.close() }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    spark.udf.register("parse_words", WordSplitterUDF.parse _)

    spark.sql("select parse_words('你好, 优秀')").show()

    spark.stop()
  }
}