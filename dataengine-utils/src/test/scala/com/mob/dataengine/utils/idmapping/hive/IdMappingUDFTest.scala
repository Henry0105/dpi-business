package com.mob.dataengine.utils.idmapping.hive

import com.mob.dataengine.utils.idmapping.IdMappingToolsV2
import org.scalatest.FunSuite

class IdMappingUDFTest extends FunSuite {
  test("combineIncr") {
    // 只有增量数据
    val res1 = IdMappingToolsV2.combineIncr(
      null, null, null, null,
      Seq("1"), Seq("1"), Seq("1"), Seq("1")
    )
    assertResult(Seq("1"))(res1._1)
    assertResult(Seq("1"))(res1._2)
    assertResult(Seq("1"))(res1._3)
    assertResult(Seq("1"))(res1._4)

    // 只有全量数据
    val res2 = IdMappingToolsV2.combineIncr(
      Seq("1"), Seq("1"), Seq("1"), Seq("1"),
      null, null, null, null
    )
    assertResult(Seq("1"))(res2._1)
    assertResult(Seq("1"))(res2._2)
    assertResult(Seq("1"))(res2._3)
    assertResult(Seq("1"))(res2._4)

    // 全量没有明文, 增量有明文
    // 1的MD5: c4ca4238a0b923820dcc509a6f75849b
    // 2的MD5: c81e728d9d4c2f636f067f89cc14862c
    val res3 = IdMappingToolsV2.combineIncr(
      Seq(""), Seq("c4ca4238a0b923820dcc509a6f75849b"), Seq("12"), Seq("1"),
      Seq("1"), Seq("c4ca4238a0b923820dcc509a6f75849b"), Seq("1"), Seq("12")
    )
    assertResult(Seq("1"))(res3._1)
    assertResult(null)(res3._2)
    assertResult(Seq("1"))(res3._3)
    assertResult(Seq("12"))(res3._4)

    // 全量没有明文, 增量有密文
    // 1的MD5: c4ca4238a0b923820dcc509a6f75849b
    // 2的MD5: c81e728d9d4c2f636f067f89cc14862c
    val res4 = IdMappingToolsV2.combineIncr(
      Seq("1"), null, Seq("12"), Seq("1"),
      Seq("1", ""), Seq("c4ca4238a0b923820dcc509a6f75849b",
        "c81e728d9d4c2f636f067f89cc14862c"), Seq("1", "2"), Seq("1", "2")
    )
    assertResult(Seq("", "1"))(res4._1)
    assertResult(Seq("c81e728d9d4c2f636f067f89cc14862c",
      "c4ca4238a0b923820dcc509a6f75849b"))(res4._2)
    assertResult(Seq("2", "1"))(res4._3)
    assertResult(Seq("2", "1"))(res4._4)

    // 全量没有明文, 增量有密文
    // 1的MD5: c4ca4238a0b923820dcc509a6f75849b
    // 2的MD5: c81e728d9d4c2f636f067f89cc14862c
    val res5 = IdMappingToolsV2.combineIncr(
      Seq("1", "2"), null, Seq("12", "1"), Seq("1", "2"),
      Seq("1", ""), Seq("c4ca4238a0b923820dcc509a6f75849b",
        "c81e728d9d4c2f636f067f89cc14862c"), Seq("1", "2"), Seq("1", "2")
    )
    assertResult(Seq("2", "1"))(res5._1)
    assertResult(null)(res5._2)
    assertResult(Seq("1", "1"))(res5._3)
    assertResult(Seq("2", "1"))(res5._4)
  }

  test("combineDevicePlat") {
    // 全量,增量都没有device
    val res1 = IdMappingToolsV2.combineDevicePlat(null, Seq("1"), null, Seq("1"))
    assertResult(Seq("1"))(res1)

    // 全量没有该数据
    val res2 = IdMappingToolsV2.combineDevicePlat(null, null, null, Seq("1"))
    assertResult(Seq("1"))(res2)

    // 增量没有该数据
    val res3 = IdMappingToolsV2.combineDevicePlat(null, Seq("1"), null, null)
    assertResult(null)(res3)

    // 全量,增量都该数据
    val res4 = IdMappingToolsV2.combineDevicePlat(Seq("d"), Seq("1"), Seq("d"), Seq("1"))
    assertResult(Seq("1"))(res4)

    val res5 = IdMappingToolsV2.combineDevicePlat(Seq("d2"), Seq("2"), Seq("d"), Seq("1"))
    assertResult(Seq("2", "1"))(res5)

    val res6 = IdMappingToolsV2.combineDevicePlat(Seq("d2"), Seq("2"), Seq("d", "d2"), Seq("1", "2"))
    assertResult(Seq("2", "1"))(res6)
  }
}
