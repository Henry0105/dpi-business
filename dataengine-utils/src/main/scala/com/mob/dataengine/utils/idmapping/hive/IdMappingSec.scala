package com.mob.dataengine.utils.idmapping.hive

import org.apache.spark.sql.SparkSession

class IdMappingSec(@transient spark: SparkSession, day: String, idType: String, test: Boolean = false)
  extends IdMappingSecBase(spark, day, idType, test) {

  val splitClause: String = (Seq("device") ++ tmFields).map(f =>
    s"""
       |split(t2.agg_$f.device, ",") as $f,
       |split(t2.agg_$f.device_tm, ",") as ${f}_tm,
       |split(t2.agg_$f.device_ltm, ",") as ${f}_ltm
       """.stripMargin).mkString(",")


  override val transformSql: String =
    s"""
       |select $idType, $selectFromCollapsedCols, duid, update_time, $day as day, plat
       |from (
       |  select $idType, $collapseClause, duid, update_time, $day as day, plat
       |  from (
       |    select
       |      $idType,
       |      $splitClause,
       |      split(t2.agg_duid.device, ",") as duid,
       |      t2.agg_device.update_time as update_time,
       |      plat
       |    from (
       |     select
       |       aggregateDevice(device,device_tm,device_ltm) as agg_device,
       |       max($idType) as $idType,
       |       $aggClause
       |       aggregateDevice(concat_ws(',', duid), '', '') as agg_duid,
       |       coalesce(plat, 1) plat
       |     from
       |       t1 group by $idType, coalesce(plat, 1)
       |    ) t2
       |  ) t3
       |) transform_tb
      """.stripMargin

  override val insertSql: String = {
    val c1 =
      s"""
         |select $idType, $selectFromCombinedCols, update_time, duid, plat
         |from (
         |  select
         |  coalesce(full_tb.$idType, incr_tb.$idType) $idType,
         |  $combineIncrClause,
         |  greatest(full_tb.update_time, incr_tb.update_time) update_time,
         |  coalesce(full_tb.plat, incr_tb.plat, 1) plat,
         |  merge_list(full_tb.duid, incr_tb.duid) duid
         |  from (
         |    select
         |      $idType,
         |      $empty2NullClause,
         |      update_time,
         |      day,
         |      coalesce(plat, 1) plat --外部交换的数据没有plat,设置一个默认的
         |    from joined_tb
         |  ) incr_tb
         |  full join (
         |    select *
         |    from $targetTable
         |    where day = '$beforeDay'  -- 前一天的全量数据
         |  ) full_tb
         |  on full_tb.$idType = incr_tb.$idType
         |) tmp
        """.stripMargin

    s"""
       |insert overwrite table $targetTable
       |select ${allFields.mkString(",")}
       |from (
       |  select $idType, $empty2NullClause,
       |    plat, update_time, '$day' day
       |  from (
       |    $c1
       |  ) tmp2
       |) result_tb
     """.stripMargin
  }
}
