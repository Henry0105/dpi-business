#!/bin/sh
: '
@owner: zhongqi
@describe: dmp画像app标签，导入hbase
@projectName:appLabel
@BusinessName:dmp
@SourceTable:rp_sdk_dmp.timewindow_online_profile 
@TargetTable:HBASE.default:rp_device_profile_info
@TableRelation:rp_sdk_dmp.timewindow_online_profile->HBASE.default:rp_device_profile_info
'

set -x -e
cd `dirname $0`



date1=$1
#计算数据分区，date1-2（t-2）
date=`date -d "$date1 -3 days" "+%Y%m%d"`

#只有每周三跑一次
#weekDay=`date -d "$date" +%w`
#if [ $weekDay -ne 1 ]; then
#	exit 0;
#fi


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
device,
concat(
concat_ws('|||', collect_list(concat(substring(feature,1,4),'_',substring(feature,5,3),'_0'))),
'=',
concat_ws('|||', collect_list(cnt))
)  as \\\`cf:installed_app_tag\\\`
from rp_sdk_dmp.timewindow_online_profile 
where flag=4 ---在装
and 
timewindow=7
and
length(trim(feature))=11
and day = $date
group  by device 
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
