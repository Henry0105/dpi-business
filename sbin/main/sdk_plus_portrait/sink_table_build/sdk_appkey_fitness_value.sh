#!/usr/bin/env bash

:'
@owner: xlmeng
@describe: appkey的设备健康值
@projectName: dataengine
@BusinessName: sdk+群体画像
@SourceTable: rp_dataengine.sdk_appkey_profile_full
@TargetTable: rp_dataengine.sdk_appkey_fitness_value
@author: xlmeng
'

:'
@parameters
@day: 任务日期
@pday: 任务日期前1天
@p90day: 任务日期前90天
@envir: 环境 0:测试环境；1:生产环境
'

set -x -e

envir=$1
day=$2
pday=$(date -d "$day -1  days" +%Y%m%d)
p90day=$(date -d "$day -90  days" +%Y%m%d)

if [ $envir == 0 ]
then
  database="rp_dataengine_test"
elif [ $envir == 1 ]
then
  database="rp_dataengine"
else
  echo "参数输入错误:"
  echo "       第一个参数必须是0或者1(0:测试环境,1:生产环境)"
  echo "       第二个参数为日期(格式:yyyyMMdd)"
  exit 2
fi

input="$database.sdk_appkey_profile_full"
output="$database.sdk_appkey_fitness_value"

hive -v -e"
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
SET mapred.max.split.size=250000000;
SET mapred.min.split.size.per.node=128000000;
SET mapred.min.split.size.per.rack=128000000;
SET hive.merge.smallfiles.avgsize=250000000;
SET hive.merge.size.per.task = 250000000;

INSERT OVERWRITE TABLE $output PARTITION ( day = '$day' )
SELECT
    appkey
    ,growth_rate_value                                    AS growth_rate_value
    ,component_ratio_value                                AS component_ratio_value
    ,(growth_rate_value + component_ratio_value) / 2      AS value
    ,today_growth_rate                                    AS user_growth_rate
FROM
    (
      SELECT
          v1.appkey
          ,v1.component_ratio_value
          ,v1.today_growth_rate
          ,CASE WHEN nvl(v2.avg_growth_rate, 0) - v1.today_growth_rate <= 0.05 THEN 100
                ELSE today_growth_rate / (avg_growth_rate - 0.05)
          END AS growth_rate_value
      FROM
          (
            SELECT
                appkey
                ,today_growth_rate
                ,(new_user_value + active_user_value) / 2 AS component_ratio_value
            FROM
                (
                  SELECT
                      appkey
                      ,today_growth_rate
                      ,CASE WHEN new_loss_ratio >= 1  THEN 100
                            WHEN new_loss_ratio < 0.5 THEN 0
                            ELSE 100 - (1 - new_loss_ratio) * 2
                      END AS new_user_value
                      ,CASE WHEN active_silence_ratio >= 1  THEN 100
                            WHEN active_silence_ratio < 0.5 THEN 0
                            ELSE 100 - (1 - active_silence_ratio) * 2
                      END AS active_user_value
                  FROM
                      (
                        SELECT
                            t1.appkey
                            ,IF( t2.appkey IS NOT NULL
                                ,IF( t1.new_user_cnt > 0
                                    ,t1.new_user_cnt / (t2.new_user_cnt + t2.active_user_cnt + t2.silence_user_cnt + t2.lost_user_cnt)
                                    ,0)
                                ,0)  AS today_growth_rate
                            ,t1.new_user_cnt / IF(t1.lost_user_cnt!=0, t1.lost_user_cnt, 1)              AS new_loss_ratio
                            ,t1.active_user_cnt / IF(t1.silence_user_cnt!=0, t1.silence_user_cnt, 1)     AS active_silence_ratio
                        FROM
                            $input  AS t1
                        LEFT JOIN
                            $input  AS t2
                        ON
                            t2.day = '$pday'
                            AND t1.appkey = t2.appkey
                        WHERE
                            t1.day = '$day'
                      ) AS t3
                ) AS t4
          ) AS v1
      LEFT JOIN
          (
            SELECT
                appkey
                ,nvl(avg(user_growth_rate) ,0)   AS avg_growth_rate
            FROM
                $output
            WHERE
                day < '$day'
                AND day >= '$p90day'
            GROUP BY
                appkey
          ) AS v2
      ON
          v1.appkey = v2.appkey
    ) AS v3
"