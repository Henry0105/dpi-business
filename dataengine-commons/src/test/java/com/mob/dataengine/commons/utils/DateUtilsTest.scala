package com.mob.dataengine.commons.utils

import com.mob.dataengine.commons.utils.DateUtils._
import org.scalatest.FlatSpec

/**
 * @author juntao zhang
 */
class DateUtilsTest extends FlatSpec {
  "获取时间" should "getLastNDays" in {
    assert(getLastNDays(Seq("20190307", "20190306"), 0) == Seq("20190306", "20190307"))
    assert(getLastNDays(Seq("20190307", "20190306"), 2) == Seq("20190304", "20190305"))
    assert(getLastNDays(Seq("20190307", "20190301"), 1) == Seq("20190228", "20190306"))
    assert(getLastNDays(Seq("20190307"), 3, containsCurr = true) ==
      Seq("20190304", "20190305", "20190306", "20190307"))
  }

  "DateUtils" should "ranges" in {
    assert(ranges("20180901", "20190317", 60) ==
      Seq(("20190116", "20190317"), ("20181116", "20190115"), ("20180916", "20181115"), ("20180901", "20180915")))
    assert(ranges("20190301", "20190317", 60) == Seq(("20190301", "20190317")))
    println(ranges("20180901", "20190317", 30))
  }
}
