package com.mob.dataengine.core.bean
import org.apache.spark.sql.LocalSparkSession
import org.scalatest._
import com.fasterxml.jackson.databind.JsonMappingException

class ApiSparkTest extends FunSuite with LocalSparkSession {
  override def beforeAll(): Unit = {
    super.beforeAll()
  }

  override def afterAll(): Unit = {
  }

  test("count test") {
    val ic = IosConfig(spark)
    ic.printSchema()
    ic.show(false)
    val count = ic.count()
    assert(count == 46)
  }
}
