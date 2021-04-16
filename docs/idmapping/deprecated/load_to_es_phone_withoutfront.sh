#!/bin/sh

set -x -e

cd `dirname $0`
testdb_phone=test
testtb_phone=phone_device_mapping_temp_zhongqi
day=$1

hive -e "create table if not exists test.phone_device_mapping_temp_zhongqi(phone string,device_id string,time string) STORED AS orcfile;"

hive -e "insert overwrite table test.phone_device_mapping_temp_zhongqi
select t.phone
,concat_ws(',', collect_list(t.device_id)),concat_ws(',', collect_list( cast(( unix_timestamp(t.update_date ,'yyyyMMdd'))  as string ))) 
from     
(
select device_id,phone_view as phone,update_date
from test.zhongqi_device_id_mapping_hive_all_temp  
lateral view explode (split(phone,',')) s as phone_view     
where phone!='' and phone is not null and update_date=$day
) t group by phone"




#phone_es
spark-submit \
--class com.mob.job.DataExport \
--master yarn-cluster \
--driver-memory 10G \
--num-executors 1 \
--executor-memory 10G \
--executor-cores 1 \
--conf spark.yarn.executor.memoryOverhead=4096 \
--conf spark.app.name=DataExport-to-test-es-phone_$day \
/data/schedule/dataexport/mob-data-export-2.0.2.jar \
"{
    \"operationName\": \"hiveToEs\",
    \"sourceInfo\": {
        \"object\": \"hive\",
        \"database\": \"${testdb_phone}\",
        \"table\": \"${testtb_phone}\",
        \"fields\": [
            \"case when phone is null then '' when trim(phone) = '' then '' else MD5_Eencrypt_UDF(lower(trim(phone)),32) end as id\",
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
        \"otherParams\": \" and length(device_id)<10000 and device_id is not null and phone is not null and time is not null\"
    },
    \"targetInfo\": {
        \"object\": \"es\",
        \"operateType\": \"rdd\",
        \"esIndex\": \"dmp_phone_deviceid\",
        \"esType\": \"dmp_phone_deviceid\",
        \"esId\": \"id\",
        \"esNodes\": \"10.6.160.121:19200,10.6.160.122:19200,10.6.160.123:19200,10.6.160.130:19200,10.6.160.131:19200\"
    }
}
"
