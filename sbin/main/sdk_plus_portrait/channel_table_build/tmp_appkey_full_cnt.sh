#!/usr/bin/env bash

:'
@owner: xlmeng
@describe: 从tmp_device_appkey_labels来的数据每个标签进行分组计算
           是全部用户类型级别的，这张表为了后续计算tgi准备
@projectName: dataengine
@BusinessName: sdk+群体画像
@SourceTable: dm_dataengine_mapping.tmp_device_appkey_labels
@TargetTable: dm_dataengine_mapping.tmp_appkey_full_cnt
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
  database="dm_dataengine_test"
elif [ $envir == 1 ]
then
  database="dm_dataengine_mapping"
else
  echo "参数输入错误:"
  echo "       第一个参数必须是0或者1(0:测试环境,1:生产环境)"
  echo "       第二个参数为日期(格式:yyyyMMdd)"
  exit 2
fi

input="$database.tmp_device_appkey_labels"
output="$database.tmp_appkey_full_cnt"

# count_full表
hive -v -e "
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
CREATE TEMPORARY FUNCTION count_partial AS 'com.youzu.mob.java.udaf.CountPartial';

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
    ,count_partial(gender)               AS gender
    ,count_partial(age)                  AS age
    ,count_partial(city_level)           AS city_level
    ,count_partial(income)               AS income
FROM
    $input
WHERE
    day = '$day'
GROUP BY
    appkey
"