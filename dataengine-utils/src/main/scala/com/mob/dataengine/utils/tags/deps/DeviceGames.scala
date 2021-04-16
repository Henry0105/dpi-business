package com.mob.dataengine.utils.tags.deps


import com.mob.dataengine.commons.annotation.code.{author, createTime, sourceTable}
import com.mob.dataengine.utils.PropUtils
import org.apache.spark.sql.DataFrame
import org.slf4j.LoggerFactory
/**
 * 游戏标签, 含标签:以G编码参考crrowd-portrait.json配置
 * rp_sdk_dmp.timewindow_online_profile|FLAG=17/游戏类新安装 对应feature中间值为0
 * rp_sdk_dmp.timewindow_online_profile|FLAG=18/游戏类新卸载 对应feature中间为1
 * rp_sdk_dmp.timewindow_online_profile|FLAG=19/游戏类活跃天数 对应feature中间为3
 * rp_sdk_dmp.timewindow_online_profile|FLAG=20/游戏类活跃个数 对应feature中间为2
 * rp_sdk_dmp.timewindow_online_profile|FLAG=21/游戏类app安装个数 对应feature中间为4
 * 新增games_tag标签字段
 * @see AbstractTimewindowProfileJob
 */
@author("xdzhang")
@createTime("2018-09-05")
@sourceTable("rp_sdk_dmp.timewindow_online_profile")
case class DeviceGames(cxt: Context) extends AbstractDataset(cxt) {
  @transient private[this] val logger = LoggerFactory.getLogger(getClass)
  val datasetId: String = s"${com.mob.dataengine.commons.utils.PropUtils.HIVE_TABLE_TIMEWINDOW_ONLINE_PROFILE_V2}_0_7"

  def _dataset(): DataFrame = {

    val day = cxt.tablePartitions(datasetId)
    val codes = PropUtils.getProperty("games.feature.code")
    val gcode = PropUtils.getProperty("games.feature.G10000.code")
    cxt.update(datasetId, day)
    sql(
      s"""
        |select device,concat_ws('\u0001',collect_set(concat(feature,'=',cnt)))
        |as games_tag, day
        |from (
        |  select device,feature,sum(cnt) cnt,day from (
        |  select device ,
        |  case when feature like '${gcode}%' then concat('G10000_',split(feature,'_')[1],'_',split(feature,'_')[2])
        |    else feature end as feature,
        |  cnt,day
        |  from ${com.mob.dataengine.commons.utils.PropUtils.HIVE_TABLE_TIMEWINDOW_ONLINE_PROFILE_V2}
        |   where flag in(0,1,2,3,4) and day ='${day}' and ${sampleClause()}
        |  and (split(feature,'_')[0] in (${codes}) or feature like '${gcode}%')
        |    )gda group by device,feature,day
        | )da
        |group by device,day
      """.stripMargin)
  }
}
