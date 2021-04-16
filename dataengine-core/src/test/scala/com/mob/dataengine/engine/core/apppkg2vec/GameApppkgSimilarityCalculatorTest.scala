package com.mob.dataengine.engine.core.apppkg2vec

import com.mob.dataengine.commons.utils.PropUtils
import org.apache.spark.sql.LocalSparkSession
import org.scalatest.FunSuite

class GameApppkgSimilarityCalculatorTest extends FunSuite with LocalSparkSession {

  import spark.implicits._

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.sql("DROP DATABASE IF EXISTS dm_dataengine_mapping CASCADE")
    spark.sql("DROP DATABASE IF EXISTS rp_dataengine CASCADE")
    spark.sql("create database dm_dataengine_mapping")
    spark.sql("create database rp_dataengine")

    prepareWarehouse()
  }

  override def afterAll(): Unit = {
    spark.sql("DROP DATABASE IF EXISTS dm_dataengine_mapping CASCADE")
    spark.sql("DROP DATABASE IF EXISTS rp_dataengine CASCADE")
  }

  def prepareWarehouse(): Unit = {
    Seq(
      ("贪玩蓝月", "com.tanwan.com", "/mount_data/logs/taptap/icon/202004贪玩蓝月_1587444479452.png",
        Seq(0.033090490080543204, 0.09763258184962482, -0.013335141591431404), "20200428", 1),
      ("贪玩蓝月", "com.tanwan2.com", "/mount_data/logs/taptap/icon/202004贪玩蓝月_1587444479452.png",
        Seq(0.033090490080543204, 0.09763258184962482, -0.013335141591431404), "20200428", 2),
      ("贪玩蓝月", "com.tanwan3.com", "/mount_data/logs/taptap/icon/202004贪玩蓝月_1587444479452.png",
        Seq(0.033090490080543204, 0.09763258184962482, -0.013335141591431404), "20200428", 4),

      ("冒险王2", "com.tencent.tmgp.anqumxw2", "/mount_data/logs/taptap/icon/202004冒险王2_1587440413256.png",
        Seq(0.28809250000000003, 0.59859425, -0.6658257499999999), "20200428", 1),
      ("冒险王2", "com.k7k7.mxw2.cxtest", "/mount_data/logs/taptap/icon/202004冒险王2_1587440413256.png",
        Seq(0.28809250000000003, 0.59859425, -0.6658257499999999), "20200428", 2),
      ("冒险王2", "com.whtgjrtt2.k7k7", "/mount_data/logs/taptap/icon/202004冒险王2_1587440413256.png",
        Seq(0.28809250000000003, 0.59859425, -0.6658257499999999), "20200428", 4),

      ("军团荣耀", "com.tencent.tmgp.legiontd", "/mount_data/logs/taptap/icon/202004军团荣耀_1587444377168.png",
        Seq(0.654825, 0.11601549999999994, -1.0394245), "20200428", 1),
      ("军团荣耀", "com.yishengkeji.legionhonor", "/mount_data/logs/taptap/icon/202004军团荣耀_1587444377168.png",
        Seq(0.654825, 0.11601549999999994, -1.0394245), "20200428", 2),

      ("弹球达人", "com.tencent.tmgp.legiontd", "/mount_data/logs/taptap/icon/202004弹球达人_1587443519412.png",
        Seq(0.190666, -1.64904, -2.718317), "20200428", 1),
      ("弹球达人", "com.diandongtianxia.ballshooter", "/mount_data/logs/taptap/icon/202004弹球达人_1587443519412.png",
        Seq(0.190666, -1.64904, -2.718317), "20200428", 4),

      ("暗影英雄", "com.evkworld.nick", "/mount_data/logs/taptap/icon/202004暗影英雄_1587443289095.png",
        Seq(-0.455998, -0.483076, 0.837631), "20200428", 2),
      ("暗影英雄", "com.xlgame.ayyx.m233", "/mount_data/logs/taptap/icon/202004暗影英雄_1587443289095.png",
        Seq(-0.455998, -0.483076, 0.837631), "20200428", 4),
    ).toDF("app_name", "apppkg", "icon_path", "features", "day", "category")
      .write.partitionBy("day", "category")
      .saveAsTable(PropUtils.HIVE_TABLE_APPPKG_VECTOR_MAPPING)
  }

  test("游戏竞品 单元测试") {
    val myJSON =
      s"""{
            "jobId":"apppkg_similarity_20200429",
            "jobName":"apppkg_similarity",
            "day":"20200428",
            "rpcHost": "localhost",
            "userId": "9090",
            "rpcPort": 0,
            "params":[
                {
                    "inputs":[
                        {
                            "idType": 4,
                            "appName": "贪玩蓝月",
                            "description": "类型转换任务参数"
                        }
                    ],
                    "output":{
                        "uuid":"example_01_out",
                        "limit":2
                    }
                }
            ]
          }"""

    GameApppkgSimilarityCalculator.main(Array(myJSON))

  }

}
