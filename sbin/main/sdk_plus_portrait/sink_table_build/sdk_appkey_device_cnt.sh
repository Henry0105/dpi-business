#!/usr/bin/env bash

:'
@owner: xlmeng
@describe: apppkg近90天的活跃活跃数
@projectName: dataengine
@BusinessName: sdk+群体画像
@SourceTable: rp_mobdi_app.device_pkg_active_label,dw_sdk_log.pv
@TargetTable: rp_dataengine.sdk_appkey_device_cnt
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
  database2="rp_dataengine_test"
elif [ $envir == 1 ]
then
  database1="rp_mobdi_app"
  database2="rp_dataengine"
else
  echo "参数输入错误:"
  echo "       第一个参数必须是0或者1(0:测试环境,1:生产环境)"
  echo "       第二个参数为日期(格式:yyyyMMdd)"
  exit 2
fi

input="$database1.ads_device_pkg_active_label"
output="$database2.sdk_appkey_device_cnt"

# 首页活跃数
hive -v -e "
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
SET mapred.max.split.size=250000000;
SET mapred.min.split.size.per.node=128000000;
SET mapred.min.split.size.per.rack=128000000;
SET hive.merge.smallfiles.avgsize=250000000;
SET hive.merge.size.per.task = 250000000;

INSERT OVERWRITE TABLE $output PARTITION (day='$day')
SELECT
    t1.apppkg, t2.appkey, t1.device_cnt, t1.plat
FROM
    (
      SELECT
          apppkg
          ,count(*)   AS device_cnt
          ,plat
      FROM $input
      WHERE
          day = '$day'
          AND cnt_day_90 > 0
      GROUP BY
          apppkg, plat
    ) AS t1
INNER JOIN
    dm_mobdi_mapping.dim_mapping_apppkg_appkey_par_df_view AS t2
ON
   t1.apppkg = t2.apppkg
"