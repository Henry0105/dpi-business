#!/bin/sh
: '
@owner: zhongqi
@describe: inital_ts hive to hbase
@projectName:
@BusinessName:
@SourceTable:
@TargetTable:HBASE.default:rp_device_profile_info
@TableRelation:
'

set -x -e
cd `dirname $0`

date=$1

: '
实现功能：在做
实现逻辑：
输出结果：
'

day=$1

#参数配置详见http://c.mob.com/pages/viewpage.action?pageId=4890346
/usr/bin/spark-submit \
--class com.mob.job.DataExport \
--jars /opt/cloudera/parcels/CDH-5.7.6-1.cdh5.7.6.p0.6/jars/hbase-spark-1.2.0-cdh5.7.6.jar \
--master yarn-cluster \
--driver-memory 10G \
--num-executors 3 \
--executor-memory 5G \
--executor-cores 1 \
--conf spark.yarn.executor.memoryOverhead=4096 \
/data/schedule/dataexport/mob-data-export-2.0.2.jar \
"{
    \"operationName\": \"hiveToHbase\",
    \"sourceInfo\": {
        \"object\": \"hive\",
        \"rowkeyField\": \"device\",
        \"sql\": \"select device as device,day as \\\`cf:inital_ts\\\` from dm_sdk_mapping.device_mintime_incr_mapping  where pt =$day\",
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
        \"zkQuorum\": \"10.6.25.107,10.6.28.99,10.6.25.98\"
    }
}
"
