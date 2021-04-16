#!/bin/sh

set -x -e

cd `dirname $0`

day=$1
source_database="test"
source_table="idfa_device_mapping_temp_zhongqi"

hive -e "create table if not exists test.idfa_device_mapping_temp_zhongqi(idfa string,device_id string,time string) STORED AS orcfile;"

hive -e "insert overwrite table test.idfa_device_mapping_temp_zhongqi     
select imei_idfa,concat_ws(',', collect_list(device_id)),concat_ws(',', collect_list( cast(( unix_timestamp(update_date ,'yyyyMMdd'))  as string )))
from test.zhongqi_device_id_mapping_hive_all_temp    
where imei_idfa!='' and imei_idfa is not null and update_date=$day
group by imei_idfa"

#idfa_es
start_time=`date +%Y%m%d-%H%M%S`
#echo "$start_time start  loadtoes_idfa$date" >> $log_path
spark-submit \
--class com.mob.job.DataExport \
--master yarn-cluster \
--driver-memory 10G \
--num-executors 1 \
--executor-memory 10G \
--executor-cores 1 \
--conf spark.yarn.executor.memoryOverhead=4096 \
--conf spark.app.name=DataExport-to-test-es-idfa_$day \
/data/schedule/dataexport/mob-data-export-2.0.2.jar \
"{
    \"operationName\": \"hiveToEs\",
    \"sourceInfo\": {
        \"object\": \"hive\",
        \"database\": \"${source_database}\",
        \"table\": \"${source_table}\",
        \"fields\": [
            \"case when idfa is null then '' when trim(idfa) = '' then '' else MD5_Eencrypt_UDF(lower(trim(idfa)),32) end as id\",
            \"device_id as deviceid\",
            \"time as updateTime\"
        ],
        \"udfs\": {
            \"MD5_Eencrypt_UDF()\": \"com.youzu.mob.java.udf.MD5EencryptUDF\",
            \"SplitAndMD5()\": \"com.youzu.mob.java.udf.SplitAndMD5\"
        },
        \"udfJars\": [
            \"hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.2.jar\",
            \"hdfs://ShareSdkHadoop/dmgroup/dba/commmon/dependencies/lib/lamfire-2.1.4.jar\"
        ],
        \"otherParams\": \" and length(device_id)<20500 \"
    },
    \"targetInfo\": {
        \"object\": \"es\",
        \"operateType\": \"rdd\",
        \"esIndex\": \"dmp_imei_idfa_deviceid\",
        \"esType\": \"dmp_imei_idfa_deviceid\",
        \"esId\": \"id\",
        \"esNodes\": \"10.6.160.121:19200,10.6.160.122:19200,10.6.160.123:19200,10.6.160.130:19200,10.6.160.131:19200\"
    }
}
"
