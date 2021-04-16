#!/bin/sh
: '
@owner: lvxl
@describe: 金融标签-在装，导入hbase
@projectName:industryLabel
@BusinessName:load_financial_installed_to_hbase
@SourceTable:rp_sdk_dmp.rp_device_financial_installed_profile
@TargetTable:HBASE.default:rp_device_profile_info
@TableRelation:rp_sdk_dmp.rp_device_financial_installed_profile->HBASE.default:rp_device_profile_info
'

set -x -e
cd `dirname $0`
libPath=/data/schedule/dataexport
date1=$1
#计算数据分区，date1-2（t-2）
date=`date -d "$date1 -2 days" "+%Y%m%d"`

#只有每周三跑一次
weekDay=`date -d "$date" +%w`
if [ $weekDay -ne 1 ]; then
	exit 0;
fi

date_1=`hive -e "select max(day) from rp_sdk_dmp.timewindow_online_profile where flag=10 and timewindow=1"`
: '
实现功能：将在装标签导入hbase

实现逻辑：将除device字段外所有字段映射成金融标签code，并以a,b=1,2的形式拼成1个字段
输出结果：hbase表rp_device_profile_info
          字段：installed_cate_tag  --金融在装标签
'


#参数配置详见http://c.mob.com/pages/viewpage.action?pageId=4890346
/usr/bin/spark-submit \
--class com.mob.job.DataExport \
--jars /opt/cloudera/parcels/CDH-5.7.6-1.cdh5.7.6.p0.6/jars/hbase-spark-1.2.0-cdh5.7.6.jar \
--master yarn-cluster \
--driver-memory 10G \
--num-executors 10 \
--executor-memory 10G \
--executor-cores 3 \
--conf spark.yarn.executor.memoryOverhead=4096 \
/data/schedule/dataexport/mob-data-export-2.0.2.jar \
"{
    \"operationName\": \"hiveToHbase\",
    \"sourceInfo\": {
        \"object\": \"hive\",
        \"rowkeyField\": \"device\",
        \"sql\": \"	
select 
(case when 
a.device is null then b.device else a.device end) as device,
FieldsToOne('F0001|F0002|F0003|F0004|F0005|F0006|F0007|F0008|F0009|F0010',
case when a.borrowing is null then '' else  a.borrowing end,
case when a.bank is null then '' else  a.bank end,
case when a.investment is null then '' else  a.investment end ,
case when a.insurance is null then '' else  a.insurance end ,
case when a.securities is null then '' else  a.securities end ,
case when a.finaces is null then '' else  a.finaces end ,
case when a.credit_card is null then '' else  a.credit_card end,
case when a.total is null then '' else  a.total end,
case when a.percent is null then '' else  a.percent end,
case when  b.cnt is null then '' else b.cnt end
) as \\\`cf:installed_cate_tag\\\` 
from(
select device ,borrowing,bank,investment,insurance,securities,finaces,credit_card,total,percent from 
rp_sdk_dmp.rp_device_financial_installed_profile where day=$date )a
full join 
(select device,cnt from rp_sdk_dmp.timewindow_online_profile where flag=10 and timewindow=1 and day = $date_1 and feature = '支付') b
on a.device = b.device
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
