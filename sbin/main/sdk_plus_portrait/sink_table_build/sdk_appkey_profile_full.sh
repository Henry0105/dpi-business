#!/usr/bin/env bash

:'
@owner: xlmeng
@describe: appkey的一级用户和二级用户类型的数量
@projectName: dataengine
@BusinessName: sdk+群体画像
@SourceTable: dm_dataengine_mapping.device_appkey_userType
@TargetTable: rp_dataengine.sdk_appkey_profile_full
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
  database1="dm_dataengine_test"
  database2="rp_dataengine_test"
elif [ $envir == 1 ]
then
  database1="dm_dataengine_mapping"
  database2="rp_dataengine"
else
  echo "参数输入错误:"
  echo "       第一个参数必须是0或者1(0:测试环境,1:生产环境)"
  echo "       第二个参数为日期(格式:yyyyMMdd)"
  exit 2
fi

input="$database1.device_appkey_userType"
output="$database2.sdk_appkey_profile_full"

# 用户数目统计
hive -v -e "
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
SET mapred.max.split.size=250000000;
SET mapred.min.split.size.per.node=128000000;
SET mapred.min.split.size.per.rack=128000000;
SET hive.merge.smallfiles.avgsize=250000000;
SET hive.merge.size.per.task = 250000000;

INSERT OVERWRITE TABLE $output PARTITION (day = '$day')
SELECT
    appkey
    ,max(nvl(kv['1-1'] ,0)) + max(NVL(kv['1-2'] ,0))    AS new_user_cnt
    ,max(nvl(kv['2-1'] ,0)) + max(NVL(kv['2-2'] ,0))    AS active_user_cnt
    ,max(nvl(kv['3-1'] ,0)) + max(NVL(kv['3-2'] ,0))    AS silence_user_cnt
    ,max(nvl(kv['4'] ,0))                          AS lost_user_cnt
    ,max(nvl(kv['1-1'] ,0))                        AS new_potential_user_cnt
    ,max(nvl(kv['1-2'] ,0))                        AS new_lostrisk_user_cnt
    ,max(nvl(kv['2-1'] ,0))                        AS active_high_user_cnt
    ,max(nvl(kv['2-2'] ,0))                        AS active_low_user_cnt
    ,nvl(max(active_silence_user_cnt), 0)          AS active_silence_user_cnt
    ,max(nvl(kv['3-1'] ,0))                        AS silence_awake_user_cnt
    ,max(nvl(kv['3-2'] ,0))                        AS silence_lostrisk_user_cnt
FROM
    (
      -- 1.流失用户
      SELECT
          appkey
          ,map('4', count(*))        AS kv
          ,0 AS active_silence_user_cnt
      FROM
          $input
      WHERE
          day = '$day'
          AND user_type = 4
      GROUP BY
          appkey

      UNION ALL
      -- 2.新增用户
      SELECT
          appkey
          ,map(user_type_2, count(*)) AS kv
          ,0 AS active_silence_user_cnt
      FROM
          (
            SELECT
                appkey
                ,CASE WHEN cnt_day_30 >= 10 THEN '1-1' -- 潜力用户:近30天内，device在这个appkey上的活跃天数大于等于10天
                      ELSE                       '1-2' -- 流失风险用户:近30天内，device在这个appkey上的活跃天数小于10天（包括近30天无活跃）
                END AS user_type_2
                ,0  AS active_silence_user_mark
            FROM
                $input
            WHERE
                day = '$day'
                AND user_type = 1
          ) AS t3
      GROUP BY
          appkey, user_type_2

      UNION ALL
      -- 3.沉默用户
      SELECT
          appkey
          ,map(user_type_2, count(*)) AS kv
          ,0 AS active_silence_user_cnt
      FROM
          (
            SELECT
                appkey
                ,CASE WHEN duration_interval_clean < 600000 THEN '3-1' -- 可唤醒用户:device在这个appkey上的最后一次存活时长小于10分钟
                      ELSE                                       '3-2' -- 流失风险用户:device在这个appkey上的最后一次存活时长大于等于10分钟
                END AS user_type_2
            FROM
                $input
            WHERE
                day = '$day'
                AND user_type = 3
          ) AS t2
      GROUP BY
          appkey, user_type_2

      UNION ALL
      -- 4.活跃用户
      SELECT
          appkey
          ,map(user_type_2, count(*)) AS kv
          ,sum(IF (active_silence_user_mark = 1, 1, 0)) AS active_silence_user_cnt
      FROM
          (
            SELECT
                appkey
                ,CASE WHEN cnt_day_90 >= 30 THEN '2-1'
                      ELSE                       '2-2'
                END                         AS user_type_2
                ,IF(cnt_day_7 = 0, 1, 0)    AS active_silence_user_mark
            FROM
                $input
            WHERE
                day = '$day'
                AND user_type = 2
          ) AS t3
      GROUP BY
          appkey, user_type_2
    ) AS t4
GROUP BY
    appkey
"