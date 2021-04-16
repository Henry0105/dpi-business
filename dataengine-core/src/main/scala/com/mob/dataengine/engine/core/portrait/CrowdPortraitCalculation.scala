package com.mob.dataengine.engine.core.portrait

import com.mob.dataengine.commons.annotation.code.{author, createTime}
import com.mob.dataengine.engine.core.jobsparam.{BaseJob, PortraitCalculationParam}
import com.mob.dataengine.engine.core.portrait.crowd.calculation._
import org.apache.log4j.Logger

// todo MDML-25-RESOLVED加入注释 自定义群体画像 这里类的命名可能不太准 群体都用crowd不要再用group(这个是我之前命名太随意了)
// todo 你斟酌一下命名 我建议都放在crowd包下面
// confluence上的标签编码链接说明建议能作为注释

/**
 * 自定义群体标签(画像)计算, 含:
 *
 * @see 基础标签: D001-D029, D034-D039<a href="http://c.mob.com/pages/viewpage.action?pageId=5665224">(Mapping码)</a>
 * @see 金融标签: F0001-F0010, F1001-F1039<a href="http://c.mob.com/pages/viewpage.action?pageId=6524151">(Mapping码)</a>
 * @see 餐饮标签: CA00-CI000<a href="http://c.mob.com/pages/viewpage.action?pageId=6524135">(Mapping码)</a>
 * @see 旅游标签: LA000-LR000<a href="http://c.mob.com/pages/viewpage.action?pageId=6524149">(Mapping码)</a>
 */
@author("juntao zhang")
@createTime("2018-06-25")
object CrowdPortraitCalculation extends BaseJob[PortraitCalculationParam] {
  @transient private[this] val logger = Logger.getLogger(this.getClass)

  /* 计算模块总数, 含预处理(输入数据解析) */
  var jobNums = 14
  override def run(): Unit = {
    // 控制是否是local模式
    val islocal = true


    /* 上下文对象 */
    val jobC = JobContext(None, jobContext, JobStatus(jobNums), useLocal = islocal)
    val sourceDataOpt: Option[SourceData] = PreDevice(jobC, None).cal("PreDevice")

    if (sourceDataOpt.isEmpty) {
      logger.info(s"PreDevice finished with empty data")
      DeviceProfileFull(jobC, sourceDataOpt).submit("DeviceProfileFull")
      System.exit(0)
    } else {
      /* D001-D029, D034-D037 */
      DeviceProfileFull(jobC, sourceDataOpt).submit("DeviceProfileFull")
      /* F0001-F0009 */
      DeviceFinancialInstalledProfile(jobC, sourceDataOpt).submit("DeviceFinancialInstalledProfile")
      /* F1001-F1007 */
      DeviceFinancialInstallProfile(jobC, sourceDataOpt).submit("DeviceFinancialInstallProfile")
      /* F1008-F1014 */
      DeviceFinancialActiveMonthProfile(jobC, sourceDataOpt).submit("DeviceFinancialActiveMonthProfile")
      /* F1015-F1021 */
      DeviceFinancialActive3MonthProfile(jobC, sourceDataOpt).submit("DeviceFinancialActive3MonthProfile")
      /* F1022-F1028 */
      DeviceFinancialActiveWeekProfile(jobC, sourceDataOpt).submit("DeviceFinancialActiveWeekProfile")
      /* F1029-F1035 */
      DeviceFinancialSlopeWeekProfile(jobC, sourceDataOpt).submit("DeviceFinancialSlopeWeekProfile")
      /* F0010, F1036-F1039 */
      TimewindowOnlineProfile(jobC, sourceDataOpt).submit("TimewindowOnlineProfile")
      /* CA000-CI000 */
      CateringLbsLabelWeekly(jobC, sourceDataOpt).submit("CateringLbsLabelWeekly")
      /* LA000-LE000, LG000-LI000, LK000 */
      TravelDaily(jobC, sourceDataOpt).submit("TravelDaily")
      /* LJ000, LL000-LR000 */
      TimewindowOfflineProfile(jobC, sourceDataOpt).submit("TimewindowOfflineProfile")
      /* LF000 */
      AppActiveWeekly(jobC, sourceDataOpt).submit("AppActiveWeekly")
      /* D038-D039 */
      DeviceOuting(jobC, sourceDataOpt).submit("DeviceOuting")

      DeviceGamesProfile(jobC, sourceDataOpt).submit("TimewindowOnlineGameProfile")
    }
  }
}
