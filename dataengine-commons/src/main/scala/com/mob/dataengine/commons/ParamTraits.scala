package com.mob.dataengine.commons

import org.json4s.jackson.JsonMethods
import org.json4s.{DefaultFormats, JField}

object ParamTraits {
  def getCompanion[T](mf: scala.reflect.Manifest[T]): Any = {
    try {
      // scalastyle:off
      val c = Class.forName(mf.runtimeClass.getName + "$")
      c.getField("MODULE$").get(c)
      // scalastyle:on
    } catch {
      case ex: Exception =>
        println(ex.getMessage, ex)
        null
    }
  }

  def getTransformer(obj: Any): PartialFunction[JField, JField] = {
    obj match {
      case x: HasTransformer => x.transformer
      case _ => dummyTransformer
    }
  }

  val dummyTransformer: PartialFunction[JField, JField] = {
    case (x, y) => (x, y)
  }

  trait HasTransformer {
    val transformer: PartialFunction[JField, JField] = dummyTransformer
  }

  trait Parsable {
    implicit val formats: DefaultFormats.type = DefaultFormats

    def parseAs[T](p: String, paths: String*)
      (implicit mf: scala.reflect.Manifest[T]): T = {
      val obj = {
        if (mf.typeArguments.nonEmpty) {
          getCompanion(mf.typeArguments.head)
        } else {
          getCompanion(mf)
        }
      }

      val tf = getTransformer(obj)

      paths.foldLeft(JsonMethods.parse(p)) { case (acc, e) => acc \ e }
        .transformField(tf)
        .extract[T](formats, mf)
    }
  }
}
