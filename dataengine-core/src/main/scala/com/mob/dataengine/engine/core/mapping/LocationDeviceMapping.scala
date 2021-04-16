package com.mob.dataengine.engine.core.mapping

import com.mob.dataengine.engine.core.jobsparam.{BaseJob, JobContext, LocationDeviceMappingParam}
import com.mob.dataengine.engine.core.location.LocationDataParser
import com.youzu.mob.scala.udf.IsPointInPolygonUDF
import org.apache.spark.sql.types.StringType
import org.slf4j.LoggerFactory

/**
 * Created with IntelliJ IDEA.
 * Description:
 * User: zhangnw
 * Date: 2018-07-27
 * Time: 14:30
 */
object LocationDeviceMapping extends BaseJob[LocationDeviceMappingParam] {

  private[this] val logger = LoggerFactory.getLogger(LocationDeviceMapping.getClass)

  override def run(): Unit = {
    jobContext.spark.udf.register("is_in", IsPointInPolygonUDF.isIn, StringType)
    LocationDataParser.dataParsing(jobContext)
  }
}
