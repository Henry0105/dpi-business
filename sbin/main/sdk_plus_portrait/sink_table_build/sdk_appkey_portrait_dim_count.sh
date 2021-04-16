#!/usr/bin/env bash

:'
@owner: xlmeng
@describe: appkey的群体画像
@projectName: dataengine
@BusinessName: sdk+群体画像
@SourceTable: dm_dataengine_mapping.tmp_appkey_userType_cnt, dm_dataengine_mapping.tmp_device_appkey_labels, rp_mobdi_app.timewindow_offline_profile_v2
@TargetTable: rp_dataengine.sdk_appkey_portrait_dim_count
@TableRelation: dm_dataengine_mapping.tmp_appkey_userType_cnt -> 基本标签
                                                              -> group_list
                dm_dataengine_mapping.tmp_device_appkey_labels, rp_mobdi_app.timewindow_offline_profile_v2 -> offline_flag(线下喜好)
                基本标签 + group_list + offline_flag + -> rp_dataengine.sdk_appkey_portrait_dim_count
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

input1="$database1.tmp_appkey_userType_cnt"
input2="$database1.tmp_device_appkey_labels"
input3="rp_mobdi_app.timewindow_offline_profile_v2"
output="$database2.sdk_appkey_portrait_dim_count"

#input3的最新分区的日期
ver=$(hive -e "show partitions $input3" | awk -F '/' '{print $2}' | awk -F '=' '{print $2}'| tail -n 1)

hive -v -e "
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
CREATE TEMPORARY FUNCTION map_to_str AS 'com.youzu.mob.java.map.MapToString';
CREATE TEMPORARY FUNCTION count_partial AS 'com.youzu.mob.java.udaf.CountPartial';
CREATE TEMPORARY FUNCTION explode_tags as 'com.youzu.mob.java.udtf.ExplodeTags';

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
    ,max(gender)               AS gender
    ,max(age)                  AS age
    ,max(permanent_province)   AS permanent_province
    ,max(permanent_city)       AS permanent_city
    ,max(city_level)           AS city_level
    ,max(occupation)           AS occupation
    ,max(edu)                  AS edu
    ,max(income)               AS income
    ,max(factory)              AS factory
    ,max(sysver)               AS sysver
    ,max(appver)               AS appver
    ,max(group_list)           AS group_list
    ,max(offline_flag)         AS offline_flag
    ,user_type
FROM
    (
      -- 1.正常标签
      SELECT
          appkey
          ,map_to_str(gender)               AS gender
          ,map_to_str(age)                  AS age
          ,map_to_str(permanent_province)   AS permanent_province
          ,map_to_str(permanent_city)       AS permanent_city
          ,map_to_str(city_level)           AS city_level
          ,map_to_str(occupation)           AS occupation
          ,map_to_str(edu)                  AS edu
          ,map_to_str(income)               AS income
          ,map_to_str(factory)              AS factory
          ,map_to_str(sysver)               AS sysver
          ,map_to_str(appver)               AS appver
          ,null                             AS group_list
          ,null                             AS offline_flag
          ,user_type
      FROM
          $input1
      WHERE
          day = '$day'

      UNION ALL
      -- 2.线上喜好
      SELECT
          appkey
          ,null                                       AS gender
          ,null                                       AS age
          ,null                                       AS permanent_province
          ,null                                       AS permanent_city
          ,null                                       AS city_level
          ,null                                       AS occupation
          ,null                                       AS edu
          ,null                                       AS income
          ,null                                       AS factory
          ,null                                       AS sysver
          ,null                                       AS appver
          ,map_to_str(count_partial(group_label))     AS group_list
          ,null                                       AS offline_flag
          ,user_type
      FROM
          (
            SELECT
                appkey
                ,group_label
                ,user_type
            FROM
                (
                  SELECT
                      appkey
                      ,group_list
                      ,user_type
                  FROM
                      $input2
                  WHERE
                      day = '$day'
                      AND group_list != 'unknown'
                ) AS t1
                LATERAL VIEW explode(split(group_list, ',')) tmp_table AS group_label
          ) AS t2
      WHERE
          trim(group_label)  <> ''
      GROUP BY
          appkey, user_type

      UNION ALL
      -- 3.线下喜好
      SELECT
          appkey
          ,null     AS gender
          ,null     AS age
          ,null     AS permanent_province
          ,null     AS permanent_city
          ,null     AS city_level
          ,null     AS occupation
          ,null     AS edu
          ,null     AS income
          ,null     AS factory
          ,null     AS sysver
          ,null     AS appver
          ,null     AS group_list
          ,concat_ws(',' ,collect_list(concat_ws('=', flag, CAST(cnt AS STRING)))) AS offline_flag
          ,user_type
      FROM
          (
            SELECT
                appkey
                ,user_type
                ,flag
                ,cnt
            FROM
                (
                  SELECT
                      appkey
                      ,user_type
                      ,flag
                      ,cnt
                      ,row_number() OVER(PARTITION BY appkey, user_type ORDER BY cnt DESC) as rn
                  FROM
                      (
                        SELECT
                            appkey
                            ,user_type
                            ,col1     AS flag
                            ,count(*) AS cnt
                        FROM
                            (
                              SELECT
                                  t1.appkey
                                  ,t1.user_type
                                  ,t2.cnt
                              FROM
                                  $input2    AS t1
                              JOIN
                                  (
                                    SELECT
                                        device
                                        ,cnt
                                    FROM
                                        $input3
                                    WHERE
                                        day = '$ver'
                                        AND timewindow = '90'
                                        AND feature IN ('facilities', 'cate1', 'brand', 'location', 'hotel_style')
                                  ) AS t2
                              ON
                                  t1.device = t2.device
                              WHERE
                                  t1.day = '$day'
                            ) AS t3
                            LATERAL VIEW explode_tags(cnt) mytable AS col1, col2
                        GROUP BY
                            appkey, user_type, col1
                      ) AS t4
                ) AS t5
            WHERE rn <= 30
          ) AS t6
      GROUP BY
          appkey, user_type

    ) AS t7
GROUP BY
    appkey, user_type
"