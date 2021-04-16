#!/usr/bin/env bash

:'
@owner: xlmeng
@describe: 生成一张appkey,device为主键的标签表
@projectName: dataengine
@BusinessName: sdk+群体画像
@SourceTable: dm_dataengine_mapping.device_appkey_userType, rp_mobdi_app.device_profile_label_full_par
@TargetTable: dm_dataengine_mapping.tmp_device_appkey_labels
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


input1="$database.device_appkey_userType"
input2="rp_mobdi_app.device_profile_label_full_par"
output="$database.tmp_device_appkey_labels"

# 获取最新分区
ver=$(hive -e "show partitions $input2" | grep ".1000" | awk -F '=' '{print $2}' | tail -n 1)

# apppkg,device的join
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
    t1.appkey
    ,t1.device
    ,t2.gender
    ,t2.agebin                      AS age
    ,CASE WHEN trim(t2.permanent_province_cn)  = '' THEN '未知'
          WHEN permanent_country = 'cn' THEN permanent_province_cn
          ELSE '外国'
    END AS permanent_province
    ,CASE WHEN trim(t2.permanent_city_cn)  = '' THEN '未知'
          WHEN permanent_country = 'cn' THEN permanent_city_cn
          ELSE '外国'
    END AS permanent_city
    ,t2.permanent_city_level        AS city_level
    ,t2.occupation
    ,t2.edu
    ,t2.income
    ,t2.cell_factory                AS factory
    ,t2.sysver
    ,t1.appver
    ,t2.group_list
    ,t1.user_type
FROM
    $input1  AS t1
JOIN
    $input2  AS t2
ON
    t2.version = '$ver'
    AND t1.device = t2.device
WHERE
    t1.day = '$day'
"
