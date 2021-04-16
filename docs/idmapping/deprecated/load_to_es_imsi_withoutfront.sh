#!/bin/sh

set -x -e

cd `dirname $0`

start_time=`date +%Y%m%d-%H%M%S`
testdb_imsi=test
testtb_imsi=imsi_device_mapping_temp_zhongqi
day=$1

hive -e "create table if not exists test.imsi_device_mapping_temp_zhongqi(imsi string,device_id string,time string) STORED AS orcfile;"

hive -e "insert overwrite table test.imsi_device_mapping_temp_zhongqi     
select t.imsi,concat_ws(',', collect_list(t.device_id)),concat_ws(',', collect_list(cast((unix_timestamp(t.update_date ,'yyyyMMdd')) as string))) 
from 
(
select device_id,imsi_view as imsi,update_date
from test.zhongqi_device_id_mapping_hive_all_temp  
lateral view explode (split(imsi,',')) s as imsi_view     
where imsi!='' and imsi is not null and update_date=$day
) t group by imsi"

start_time=`date +%Y%m%d-%H%M%S`
#imsi_es
spark-submit \
--class com.mob.job.DataExport \
--master yarn-cluster \
--driver-memory 10G \
--num-executors 2 \
--executor-memory 10G \
--executor-cores 1 \
--conf spark.yarn.executor.memoryOverhead=4096 \
--conf spark.app.name=DataExport-to-test-es-imsi_$day \
/data/schedule/dataexport/mob-data-export-2.0.2.jar \
"{
    \"operationName\": \"hiveToEs\",
    \"sourceInfo\": {
        \"object\": \"hive\",
        \"database\": \"${testdb_imsi}\",
        \"table\": \"${testtb_imsi}\",
        \"fields\": [
            \"case when imsi is null then '' when trim(imsi) = '' then '' else MD5_Eencrypt_UDF(lower(trim(imsi)),32) end as id\",
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
        \"otherParams\": \" and length(device_id)<20500\"
    },
    \"targetInfo\": {
        \"object\": \"es\",
        \"operateType\": \"rdd\",
        \"esIndex\": \"dmp_imsi_deviceid\",
        \"esType\": \"dmp_imsi_deviceid\",
        \"esId\": \"id\",
        \"esNodes\": \"10.6.160.121:19200,10.6.160.122:19200,10.6.160.123:19200,10.6.160.130:19200,10.6.160.131:19200\"
    }
}
"
end_time=`date +%Y%m%d-%H%M%S`

