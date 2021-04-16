package com.mob.dataengine.engine.core.crowd

import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.util.parsing.combinator.syntactical.StandardTokenParsers

/**
 * @author juntao zhang
 */

case class CrowdSetOptsParser(
  opt: String,
  m: mutable.HashMap[String, RDD[String]]
) extends StandardTokenParsers {

  lexical.delimiters ++= List("(", ")", "&", "-", "|")

  private val reduceList: RDD[String] ~ List[String ~ RDD[String]] => RDD[String] = {
    case i ~ ps => (i /: ps) (reduce)
  }

  // TODO (a|c) - a & (a|c) (a|c)需要cache
  private def reduce(x: RDD[String], r: String ~ RDD[String]): RDD[String] = r match {
    case "|" ~ y => x union y
    case "&" ~ y => x intersection y
    case "-" ~ y => x subtract y
  }

  private def expr: Parser[RDD[String]] = factor ~ rep("-" ~ factor | "&" ~ factor | "|" ~ factor) ^^ reduceList

  private def factor: Parser[RDD[String]] = "(" ~> expr <~ ")" | (ident | numericLit) ^^ {
    x => m(x.trim)
  }

  def run(): Option[RDD[String]] = {
    val tokens = new lexical.Scanner(opt)
    println(opt)
    val r = phrase(expr)(tokens)
    if (r.successful) {
      Some(r.get.distinct())
    } else {
      println(r)
      None
    }
  }
}
