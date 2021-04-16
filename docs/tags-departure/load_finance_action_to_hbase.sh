#!/bin/sh
: '
@owner: lvxl
@describe:金融标签-划窗 ,导入hbase
@projectName:industryLabel
@BusinessName:load_finance_action_to_hbase
@SourceTable:rp_sdk_dmp.rp_device_financial_install_profile,rp_sdk_dmp.rp_device_financial_active_month_profile,rp_sdk_dmp.rp_device_financial_active_3month_profile,rp_sdk_dmp.rp_device_financial_active_week_profile,rp_sdk_dmp.rp_device_financial_slope_week_profile
@TargetTable:HBASE.default:rp_device_profile_info
@TableRelation:rp_sdk_dmp.rp_device_financial_install_profile,rp_sdk_dmp.rp_device_financial_active_month_profile,rp_sdk_dmp.rp_device_financial_active_3month_profile,rp_sdk_dmp.rp_device_financial_active_week_profile,rp_sdk_dmp.rp_device_financial_slope_week_profile->HBASE.default:rp_device_profile_info
'
set -x -e

date1=$1
#计算数据分区，date1-3 （t-3）
day=`date -d "$date1" "+%Y%m%d"`
day_1=`hive -e "select max(day) from rp_sdk_dmp.timewindow_online_profile where flag=14 and timewindow=30"`
day_2=`hive -e "select max(day) from rp_sdk_dmp.timewindow_online_profile where flag=15 and timewindow=30"`
cd `dirname $0`
libPath=../../../lib

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
/data/schedule/dataexport/mob-data-export-2.0.2.jar \
"{
    \"operationName\": \"hiveToHbase\",
    \"sourceInfo\": {
        \"object\": \"hive\",
        \"rowkeyField\": \"device\",
        \"sql\": \"
select coalesce(cc.device,e.device) as device, cast($day as string) as \\\`cf:finance_time\\\`, 
FieldsToOne('F1001|F1002|F1003|F1004|F1005|F1006|F1007|F1008|F1009|F1010|F1011|F0012|F1013|F1014|F1015|F1016|F1017|F1018|F1019|F1020|F1021|F1022|F1023|F1024|F1025|F1026|F1027|F1028|F1029|F1030|F1031|F1032|F1033|F1034|F1035|F1036|F1037|F1038|F1039',
borrowing1,bank2,investment3,finaces4,securities5,insurance6,total7,insurance8,borrowing9,bank10,investment11,securities12,
finaces13,total14,insurance15,borrowing16,bank17,investment18,securities19,finaces20,total21,insurance22,borrowing23,bank24,
investment25,securities26,finaces27,total28,insurance_slope29,borrowing_slope30,bank_slope31,investment_slope32,
securities_slope33,finaces_slope34,total_slope35
,
coalesce(f.cnt,''),
coalesce(g.cnt,''),
coalesce(h.cnt,''),
coalesce(i.cnt,'')
) as \\\`cf:finance_action\\\`
from (
select coalesce(bb.device,d.device) as device,borrowing1,bank2,investment3,finaces4,securities5,insurance6,total7,insurance8,
borrowing9,bank10,investment11,securities12,finaces13,total14,insurance15,borrowing16,bank17,investment18,securities19,
finaces20,total21,insurance22,borrowing23,bank24,investment25,securities26,finaces27,total28 
from (
select coalesce(aa.device,c.device) as device,borrowing1,bank2,investment3,finaces4,securities5,insurance6,total7,insurance8,
borrowing9,bank10,investment11,securities12,finaces13,total14,insurance15,borrowing16,bank17,investment18,securities19,finaces20,total21 
from (
select coalesce(a.device,b.device) as device,borrowing1,bank2,investment3, finaces4,securities5,
insurance6,total7,insurance8,borrowing9,bank10,investment11,securities12,finaces13,total14 
from (
select device,borrowing as borrowing1,bank as bank2,investment as investment3,finaces as finaces4,
securities as securities5,insurance as insurance6,total as total7 
from rp_sdk_dmp.rp_device_financial_install_profile 
where day=$day    
)a
full outer join (
select device,insurance as insurance8,borrowing as borrowing9,bank as bank10,investment as investment11,
securities as securities12,finaces as finaces13,total as total14  
from rp_sdk_dmp.rp_device_financial_active_month_profile 
where day=$day 
)b on a.device=b.device 
) aa
full outer join (
select device,insurance as insurance15,borrowing as borrowing16,bank as bank17,investment as investment18,
securities as securities19,finaces as finaces20,total as total21  
from rp_sdk_dmp.rp_device_financial_active_3month_profile 
where day=$day
) c on aa.device=c.device 
) bb
full outer join (
select device,insurance as insurance22,borrowing as borrowing23,bank as bank24,investment as investment25,
securities as securities26,finaces as finaces27,total as total28  
from rp_sdk_dmp.rp_device_financial_active_week_profile 
where day=$day
  )d on bb.device=d.device
) cc
full outer join (
select device,insurance_slope as insurance_slope29,borrowing_slope as borrowing_slope30,bank_slope as bank_slope31,
investment_slope as investment_slope32,securities_slope as securities_slope33,finaces_slope as finaces_slope34,total_slope as total_slope35  
from rp_sdk_dmp.rp_device_financial_slope_week_profile 
where day=$day
) e on cc.device=e.device
full outer join(
select device,cnt from rp_sdk_dmp.timewindow_online_profile where flag=14 and timewindow=30 and feature = '理财' and day = $day_1
) f on cc.device = f.device
full outer join(
select device,feature,cnt from rp_sdk_dmp.timewindow_online_profile where flag=14 and timewindow=30 and feature = '借贷' and day = $day_1
) g on cc.device = g.device
full outer join (
select device,feature,cnt from rp_sdk_dmp.timewindow_online_profile where flag=14 and timewindow=30 and feature = '信用卡' and day = $day_1
) h on cc.device = h.device
full outer join (
select device,feature,cnt from rp_sdk_dmp.timewindow_online_profile where flag=15 and timewindow=30 and feature = '信用卡' and day = $day_2
) i on cc.device = i.device
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
