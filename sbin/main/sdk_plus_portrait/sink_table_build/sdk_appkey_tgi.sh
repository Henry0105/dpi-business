#!/usr/bin/env bash

:'
@owner: xlmeng
@describe: appkey的tgi指数
@projectName: dataengine
@BusinessName: sdk+群体画像
@SourceTable: dm_dataengine_mapping.tmp_appkey_userType_cnt, dm_dataengine_mapping.tmp_appkey_full_cnt
@TargetTable: rp_dataengine.sdk_appkey_tgi
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
input2="$database1.tmp_appkey_full_cnt"
output="$database2.sdk_appkey_tgi"

# tgi指数计算
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

add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
CREATE TEMPORARY FUNCTION map_to_str AS 'com.youzu.mob.java.map.MapToString';
CREATE TEMPORARY FUNCTION map_fill AS 'com.youzu.mob.java.map.MapFill';
CREATE TEMPORARY FUNCTION map_tgi AS 'com.youzu.mob.java.map.MapTgi';

INSERT OVERWRITE TABLE $output PARTITION (day = '$day', user_type)
SELECT
    t1.appkey
    ,map_to_str(map_fill(map_tgi(t1.gender, t2.gender), '0,1'))                   AS gender
    ,map_to_str(map_fill(map_tgi(t1.age, t2.age), '5,6,7,8,9'))                   AS age
    ,map_to_str(map_fill(map_tgi(t1.city_level, t2.city_level), '1,2,3,4,5'))     AS city_level
    ,map_to_str(map_fill(map_tgi(t1.income, t2.income), '3,4,5,6,7'))             AS income
    ,t1.user_type
FROM
    $input1 AS t1
JOIN
    $input2 AS t2
ON
    t2.day = '$day'
    AND t1.appkey = t2.appkey
WHERE
    t1.day = '$day'
"