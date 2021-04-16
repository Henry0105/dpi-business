#!/usr/bin/env bash

:'
@owner: xlmeng
@describe: 4张表与sdkMapping join从apppkg、device为主键换成appkey、device为主键
           聚合后根据条件判断用户类型，并保留部分字段用来给后续判断二级用户类型
@projectName: dataengine
@BusinessName: sdk+群体画像
@SourceTable: rp_mobdi_app.ads_sdkMapping_appkey_apppkg, rp_mobdi_app.ads_device_pkg_active_label, rp_mobdi_app.ads_device_pkg_unstall_status,  rp_mobdi_app.ads_device_pkg_install_status, rp_mobdi_app.ads_device_pkg_active_duration_full
@TargetTable: dm_dataengine_mapping.device_appkey_userType
@author: xlmeng
'

:'
@parameters
@day: 任务日期
@envir: 环境 0:测试环境；1:生产环境
'

set -x -e

envir=$1
day=$2

if [ $envir == 0 ]
then
  database1="test"
  database2="dm_dataengine_test"
elif [ $envir == 1 ]
then
  database1="rp_mobdi_app"
  database2="dm_dataengine_mapping"
else
  echo "参数输入错误:"
  echo "       第一个参数必须是0或者1(0:测试环境,1:生产环境)"
  echo "       第二个参数为日期(格式:yyyyMMdd)"
  exit 2
fi

mapping="$database1.ads_sdkMapping_appkey_apppkg"
input1="$database1.ads_device_pkg_active_label"
input2="$database1.ads_device_pkg_unstall_status"
input3="$database1.ads_device_pkg_install_status"
input4="$database1.ads_device_pkg_active_duration_full"
output="$database2.device_appkey_userType"

hive -v -e "
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
SET mapred.max.split.size=250000000;
SET mapred.min.split.size.per.node=128000000;
SET mapred.min.split.size.per.rack=128000000;
SET hive.merge.smallfiles.avgsize=250000000;
SET hive.merge.size.per.task = 250000000;
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions.pernode = 1000;
SET hive.exec.max.dynamic.partitions=1000;

INSERT OVERWRITE TABLE $output PARTITION (day = '$day', user_type)
SELECT
    appkey
    ,device
    ,appver
    ,cnt_day_90
    ,cnt_day_30
    ,cnt_day_7
    ,duration_interval_clean
    ,CASE WHEN  cnt_day_90 = 0 OR unstall_flag = 1  THEN 4 -- 流失用户:device在这个appkey上的最终状态是卸载或近90天在这个appkey上无活跃
          WHEN  install_flag > 0                    THEN 1 -- 新增用户:非流失用户，且近30天内，device在这个appkey上出现过新安装状态
          WHEN  cnt_day_90 > 0 AND cnt_day_90 < 3   THEN 3 -- 沉默用户:非流失用户，非新增用户，且device在这个appkey上近90天活跃天数大于0天小于3天
          WHEN  cnt_day_90 >= 3                     THEN 2 -- 活跃用户:其他用户，即非流失用户，非新增用户，且device在这个appkey上近90天活跃天数大于等于3天
          ELSE                                           0
    END     AS user_type
FROM
    (
      SELECT
          appkey
          ,device
          ,nvl(MAX(appver), '未知')        AS appver
          ,max(cnt_day_90)                AS cnt_day_90
          ,max(cnt_day_30)                AS cnt_day_30
          ,max(cnt_day_7)                 AS cnt_day_7
          ,max(unstall_flag)              AS unstall_flag
          ,max(install_flag)              AS install_flag
          ,max(duration_interval_clean)   AS duration_interval_clean
      FROM
          (
            -- 1.活跃天数表
            SELECT
                t1.appkey
                ,t1.apppkg
                ,t2.device
                ,t2.appver      AS appver
                ,t2.cnt_day_90
                ,t2.cnt_day_30
                ,t2.cnt_day_7
                ,0              AS unstall_flag
                ,0              AS install_flag
                ,0              AS duration_interval_clean
            FROM
                $mapping    AS t1
            JOIN
                $input1     AS t2
            ON
                t2.day = '$day'
                AND t1.apppkg = t2.apppkg
            WHERE
                t1.day = '$day'

            UNION ALL
            -- 2.设备一年内最终状态是否为卸载
            SELECT
                t1.appkey
                ,t1.apppkg
                ,t2.device
                ,null       AS appver
                ,0          AS cnt_day_90
                ,0          AS cnt_day_30
                ,0          AS cnt_day_7
                ,t2.flag    AS unstall_flag
                ,0          AS install_flag
                ,0          AS duration_interval_clean
            FROM
                $mapping    AS t1
            JOIN
                $input2     AS t2
            ON
                t2.day = '$day'
                AND t1.apppkg = t2.apppkg
            WHERE
                t1.day = '$day'

            UNION ALL
            -- 3.device,pkg,近30天出现新安装
            SELECT
                t1.appkey
                ,t1.apppkg
                ,t2.device
                ,null       AS appver
                ,0          AS cnt_day_90
                ,0          AS cnt_day_30
                ,0          AS cnt_day_7
                ,0          AS unstall_flag
                ,t2.flag    AS install_flag
                ,0          AS duration_interval_clean
            FROM
                $mapping    AS t1
            JOIN
                $input3     AS t2
            ON
                t2.day = '$day'
                AND t1.apppkg = t2.apppkg
            WHERE
                t1.day = '$day'

          UNION ALL
            -- 4.时长
            SELECT
                t1.appkey
                ,t1.apppkg
                ,t2.device
                ,null                           AS appver
                ,0                              AS cnt_day_90
                ,0                              AS cnt_day_30
                ,0                              AS cnt_day_7
                ,0                              AS unstall_flag
                ,0                              AS install_flag
                ,t2.duration_interval_clean     AS duration_interval_clean
            FROM
                $mapping    AS t1
            JOIN
                $input4     AS t2
            ON
                t2.day = '$day'
                AND t2.duration_interval_clean is not null
                AND t1.apppkg = t2.apppkg
            WHERE
                t1.day = '$day'
          ) AS t1
      GROUP BY
          appkey, device
    ) AS t2
"