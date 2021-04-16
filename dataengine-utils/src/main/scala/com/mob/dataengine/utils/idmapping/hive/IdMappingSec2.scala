package com.mob.dataengine.utils.idmapping.hive

import com.mob.dataengine.utils.DateUtils
import org.apache.spark.sql.SparkSession

class IdMappingSec2(@transient spark: SparkSession, day: String, idType: String, test: Boolean = false)
  extends IdMappingSecBase(spark, day, idType, test) {

  val splitClause: String = if (tmFields.nonEmpty) {
    tmFields.map(f =>
      s"""
         |split(t2.agg_$f.device, ",") as $f,
         |split(t2.agg_$f.device_tm, ",") as ${f}_tm,
         |split(t2.agg_$f.device_ltm, ",") as ${f}_ltm
       """.stripMargin).mkString(",") + ","
  } else {
    ""
  }

  override val transformSql: String =
    s"""
       |select $idType, $selectFromCollapsedCols, duid, update_time, $day as day, device_plat
       |from (
       |  select $idType, $collapseClause, duid, update_time, $day as day, device_plat
       |  from (
       |    select
       |      $idType,
       |      split(t2.agg_device.device,",") as device,
       |      split(t2.agg_device.device_tm,",") as device_tm,
       |      get_tm(split(t2.agg_device.device_ltm,","), 0) as device_ltm,
       |      get_tm(split(t2.agg_device.device_ltm,","), 1) as device_plat,
       |      $splitClause
       |      split(t2.agg_duid.device, ",") as duid,
       |      split(t2.agg_device.update_time, '\u0001')[0] as update_time,
       |      $day as day
       |    from (
       |     select
       |       aggregateDevice(device, device_tm, concat_ws('\u0001', device_ltm, coalesce(plat, 1))) as agg_device,
       |       max($idType) as $idType,
       |       $aggClause
       |       aggregateDevice(concat_ws(',', duid), '', '') as agg_duid
       |     from
       |       t1 group by $idType
       |    ) t2
       |  ) t3
       |) as transform_tb
      """.stripMargin


  override val insertSql: String = {
    val c1 =
      s"""
         |select $idType, $selectFromCombinedCols,
         |  update_time, duid, coalesce(device_plat, array(1)) device_plat
         |from (
         |  select
         |  coalesce(full_tb.$idType, incr_tb.$idType) $idType,
         |  combine_device_plat(full_tb.device, full_tb.device_plat,
         |    incr_tb.device, incr_tb.device_plat) device_plat,
         |  merge_list(full_tb.duid, incr_tb.duid) duid,
         |  $combineIncrClause,
         |  greatest(full_tb.update_time, incr_tb.update_time) update_time
         |  from (
         |    select
         |      $idType,
         |      coalesce(device_plat, array(1)) device_plat, --外部交换的数据没有plat,设置一个默认的
         |      $empty2NullClause,
         |      update_time,
         |      day
         |    from joined_tb
         |  ) incr_tb
         |  full join (
         |    select *
         |    from $targetTable
         |    where day = '$beforeDay'
         |  ) full_tb
         |  on full_tb.$idType = incr_tb.$idType
         |) tmp
        """.stripMargin

    s"""
       |insert overwrite table $targetTable
       |select ${allFields.mkString(",")}
       |from (
       |  select $idType, $empty2NullClause, update_time,
       |    device_plat, '$day' day
       |  from (
       |    $c1
       |  ) tmp2
       |) result_tb
     """.stripMargin
  }
}
