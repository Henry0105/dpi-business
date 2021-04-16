#! /bin/bash

if [ $# != 2 ] ; then 
 echo " wrong arguments,there should 2 date" 
exit 1; 
fi 



first=$1
second=$2  

while [ "$first" != "$second" ]  
do  
start_time=`date +%Y%m%d-%H%M%S`
echo "$first start doing temp table time:$start_time"

hive -e "create table if not exists test.zhongqi_device_id_mapping_hive_all_temp like rp_dmp_device.device_id_mapping_hive;"
echo "+++++++++++++++++++++++++++++++++++++++++++++start "$first"+++++++++++++++++++++++++++++++++++++++++"
hive -e "insert overwrite table test.zhongqi_device_id_mapping_hive_all_temp select * from rp_dmp_device.device_id_mapping_hive where update_date=$first"
end_time=`date +%Y%m%d-%H%M%S`
echo "$first  end doing temp table time:$end_time"

  
start_time=`date +%Y%m%d-%H%M%S`
echo "$first start doing mac time:$start_time"
sh load_to_es_mac_withoutfront.sh  $first
end_time=`date +%Y%m%d-%H%M%S`
echo "$first end doing mac time:$end_time"

start_time=`date +%Y%m%d-%H%M%S`
echo "$first start doing imsi time:$start_time"
sh load_to_es_imsi_withoutfront.sh  $first
end_time=`date +%Y%m%d-%H%M%S`
echo "$first end doing imsi time:$end_time"

start_time=`date +%Y%m%d-%H%M%S`
echo "$first start doing idfa time:$start_time"
sh load_to_es_idfa_withoutfront.sh  $first
end_time=`date +%Y%m%d-%H%M%S`
echo "$first end doing idfa time:$end_time"


start_time=`date +%Y%m%d-%H%M%S`
echo "$first start doing phone time:$start_time"
sh load_to_es_phone_withoutfront.sh  $first
end_time=`date +%Y%m%d-%H%M%S`
echo "$first end doing phone time:$end_time"


echo "================================success "$first"========================================"
let first=`date -d "-1 days ago ${first}" +%Y%m%d`  
done  
