#!/bin/sh


set -x -e

cd `dirname $0`

day=$1
testdb_mac=test
testtb_mac=mac_device_mapping_temp_zhongqi

hive -e "create table if not exists test.mac_device_mapping_temp_zhongqi(mac string,device_id string,time string) STORED AS orcfile;"
hive -e "insert overwrite table test.mac_device_mapping_temp_zhongqi    
select mac,concat_ws(',', collect_list(device_id)),concat_ws(',', collect_list( cast(( unix_timestamp(update_date ,'yyyyMMdd'))  as string )))     
from test.zhongqi_device_id_mapping_hive_all_temp      
where mac!='' and mac is not null and update_date=$day
group by mac;"




#mac_es
spark-submit \
--class com.mob.job.DataExport \
--master yarn-cluster \
--driver-memory 10G \
--num-executors 1 \
--executor-memory 10G \
--executor-cores 1 \
--conf spark.yarn.executor.memoryOverhead=4096 \
--conf spark.app.name=DataExport-to-test-es-mac_$day \
/data/schedule/dataexport/mob-data-export-2.0.2.jar \
"{
    \"operationName\": \"hiveToEs\",
    \"sourceInfo\": {
        \"object\": \"hive\",
        \"database\": \"${testdb_mac}\",
        \"table\": \"${testtb_mac}\",
        \"fields\": [
            \"case when mac is null then '' when trim(mac) = '' then '' else MD5_Eencrypt_UDF(lower(trim(mac)),32) end as id\",
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
        \"otherParams\": \" and length(device_id)<10000\"
    },
    \"targetInfo\": {
        \"object\": \"es\",
        \"operateType\": \"rdd\",
        \"esIndex\": \"dmp_mac_deviceid\",
        \"esType\": \"dmp_mac_deviceid\",
        \"esId\": \"id\",
        \"esNodes\": \"10.6.160.121:19200,10.6.160.122:19200,10.6.160.123:19200,10.6.160.130:19200,10.6.160.131:19200\"
    }
}
"

