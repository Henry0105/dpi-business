package com.mob.dataengine.engine.core.crowd

import com.mob.dataengine.commons.traits.UDFCollections
import com.mob.dataengine.commons.Transform
import com.mob.dataengine.engine.core.jobsparam.{BaseJob2, DataCleaningParam, JobContext2}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, concat_ws, lit}
import org.apache.spark.sql._


object DataCleaning extends BaseJob2[DataCleaningParam] with UDFCollections with Serializable {

  override def cacheInput: Boolean = true

  override def transformData(df: DataFrame, ctx: JobContext2[DataCleaningParam]): DataFrame = {
    val transform: Transform = Transform(ctx.param.output.transform)
    val outputDecodeCleanUDF: UserDefinedFunction = org.apache.spark.sql.functions.udf(transform.execute _)
    var transformDf = df.withColumn(s"${ctx.param.fieldName}", df(s"${ctx.param.origJoinField}"))

    if (ctx.param.output.cleanImei != 0) {
      transformDf = transformDf.filter(
        !transformDf(s"${ctx.param.fieldName}").rlike("[a-zA-Z]+")
      )
    }
    if (ctx.param.output.regexpClean.isDefined) {
      transformDf = transformDf.filter(!transformDf(s"${ctx.param.fieldName}")
        .rlike(ctx.param.output.regexpClean.get))
    }
    transformDf.withColumn(
      s"${ctx.param.fieldName}",
      outputDecodeCleanUDF(transformDf(s"${ctx.param.fieldName}"))
    )
  }

  override def createMatchIdsCol(ctx: JobContext2[DataCleaningParam]): Seq[Column] = {
    Seq(lit(ctx.param.inputIdTypeEnum.id), concat_ws(",", col(s"${ctx.param.fieldName}")))
  }
}
