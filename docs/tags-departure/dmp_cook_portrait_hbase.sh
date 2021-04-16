#!/bin/sh
: '
@owner: zhongqi
@describe: 餐饮标签，导入hbase,每月更新一次
@projectName:industryLabel
@BusinessName:load_cook_installed_to_hbase
@SourceTable:dm_sdk_master.catering_lbs_label_monthly
@TargetTable:HBASE.default:rp_device_profile_info
@TableRelation:dm_sdk_master.catering_lbs_label_monthly->HBASE.default:rp_device_profile_info
'

set -x -e
cd `dirname $0`
date=$1

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
		select device, 
			concat('CA000|||CB000=',
			catering_dinein_tyle1_detail,
			'|||',
			catering_dinein_tyle2_detail ) as \\\`cf:portrait_cook_tag\\\`
			from dm_sdk_master.catering_lbs_label_monthly where catering_dinein_tyle1_detail is not null
			and catering_dinein_tyle2_detail is not null and dt = $date \",
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
