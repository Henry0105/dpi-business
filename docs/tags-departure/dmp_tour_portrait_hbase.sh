#!/bin/sh
: '
@owner: zhongqi
@describe: dmp 旅游标签，导入hbase
@projectName:tourLabel
@BusinessName:load_cook_installed_to_hbase
@SourceTable:rp_sdk_dmp.timewindow_offline_profile
@TargetTable:HBASE.default:rp_device_profile_info
@TableRelation:rp_sdk_dmp.timewindow_offline_profile->HBASE.default:rp_device_profile_info
'

set -x -e
cd `dirname $0`

date1=$1
#计算数据分区，date1-2（t-2）
date=`date -d "$date1 -3 days" "+%Y%m%d"`

#只有每周三跑一次
weekDay=`date -d "$date" +%w`
if [ $weekDay -lt 1 ]; then
	exit 0;
fi



: '
实现功能：将在装标签导入hbase
实现逻辑：将除device字段外所有字段映射成旅游标签code，并以a,b=1,2的形式拼成1个字段
输出结果：hbase表rp_device_profile_info
          字段：installed_cook_tag  --餐饮在装标签
'

#参数配置详见http://c.mob.com/pages/viewpage.action?pageId=4890346
spark-submit \
--class com.mob.job.DataExport \
--jars /opt/cloudera/parcels/CDH-5.7.6-1.cdh5.7.6.p0.6/jars/hbase-spark-1.2.0-cdh5.7.6.jar \
--master yarn-cluster \
--driver-memory 10G \
--num-executors 10 \
--executor-memory 10G \
--executor-cores 1 \
--conf spark.yarn.executor.memoryOverhead=4096 \
../../../lib/mob-data-export-2.0.2.jar \
"{
    \"operationName\": \"hiveToHbase\",
    \"sourceInfo\": {
        \"object\": \"hive\",
        \"rowkeyField\": \"device\",
        \"sql\": \"

select
b.device as device,
CONCAT('LA000|||LB000|||LC000|||LD000|||LG000|||LH000|||LI000|||LJ000=',
c.travel_area,-----大区
'|||',
c.country,-----国家
'|||',
case when (business_flag=1 or busi_app_act =1) then 'business'
when (poi_flag=1 and travel_app_act=1)  or car=1 then 'tourself'
when poi_flag =1 and (business_flag=0 and  busi_app_act =0 and poi_flag=0 and travel_app_act=0 and car=0) then 'tourteam'
else 'unknown' end, -----出行类型
'|||',
c.vaca_flag,----出行时段
'|||',
c.province_flag,------大陆地区偏好

'|||',
concat_ws('',b.9_province_30),-----省份
'|||',
case when concat_ws('',b.9_city_30) like '%中国%' then concat_ws('',b.9_province_30) else concat_ws('',b.9_city_30) end ,------城市
'|||',
concat_ws('',b.9_cate1_30) -----景区
) as \\\`cf:portrait_tour_tag\\\`
from 
    (
select device,
collect_list(a.group_map['9_province_30']) as 9_province_30,
collect_list(a.group_map['9_city_30']) as 9_city_30,
collect_list(a.group_map['9_cate1_30']) as 9_cate1_30
from (
select
device,
map(feature,cnt) as group_map 
from 
rp_sdk_dmp.timewindow_offline_profile 
where flag=9 and day=$date and timewindow=30
) a
group by
a.device
) b
join 
dm_sdk_master.travel_daily c
on b.device = c.device
		\",
        \"udfs\": {
            \"FieldsToOne()\": \"com.youzu.mob.java.udf.FieldsToOne\"
        },
        \"udfJars\": [
            \"hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.1.jar\",
            \"hdfs://ShareSdkHadoop/dmgroup/dba/commmon/dependencies/lib/lamfire-2.1.4.jar\"
        ]
    },
    \"targetInfo\": {
        \"object\": \"hbase\",
        \"table\": \"rp_device_profile_info\",
        \"zkQuorum\": \"10.6.160.98,10.6.160.99,10.6.160.107\"
    }
}
"
