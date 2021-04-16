#!/bin/sh

: '
@owon:zhongan
@describe:将dmp需要的数据导入到hbase中,每天凌晨十五运行。如果dailyrun挂了超过三天就需要重跑脚本。
@projectName:DMP
@businessName:load_data_to_Hbase
@SourceTable:rp_sdk_dmp.imei_device_tag_codis_increment,rp_sdk_dmp.rp_device_profile_incr
@TargetTable:HBASE.default:rp_device_profile_info
@TableRelation:rp_sdk_dmp.imei_device_tag_codis_increment,rp_sdk_dmp.rp_device_profile_incr->HBASE.default:rp_device_profile_info
'

: '
有2个hbase集群，相同的数据各刷1遍，199集群和84(120)集群
'

set -x -e
cd `dirname $0`
libPath=../../../lib
date_temp=$1
DATE=`date -d "3 days ago $date_temp" "+%Y%m%d"`

#以下是84(120)集群

: '
实现功能：
        将rp_sdk_dmp.rp_device_profile_incr 表中的增量数据导入到Hbase表rp_device_profile_info中
实现逻辑：
        将下列字段插入到hbase中
输出字段
device                          --设备号
gender                          --性别
agebin                          --年龄
segment                         --人物标签
edu                             --教育
kids                            --有无孩子
income                          --收入
model_level                     --设备级别
tot_install_apps                --总装量
tags                            --标签
model                           --设备型号
carrier                         --运营商代码
network                         --网络
screensize                      --分辨率
sysver                          --操作系统
city_level                      --城市等级(1 - 一线城市;  2 - 二线城市;   3 - 三线城市;    4 - 四线城市;    5 - 五线城市;    -1 - 未知)
permanent_country               --常驻国家
permanent_province              --常驻省份
permanent_city                  --常驻城市
occupation                      --职业
house                           --房产情况
repayment                       --偿还能力
car                             --车产情况
workplace                       --工作地
residence                       --居住地
married                         --婚姻状况
'
/usr/bin/spark-submit \
--class com.mob.job.DataExport \
--jars /opt/cloudera/parcels/CDH-5.7.6-1.cdh5.7.6.p0.6/jars/hbase-spark-1.2.0-cdh5.7.6.jar \
--queue dmpots \
--master yarn-cluster \
--driver-memory 10G \
--num-executors 5 \
--executor-memory 10G \
--executor-cores 6 \
--conf spark.yarn.executor.memoryOverhead=10240 \
--conf spark.app.name=DataExport-tohbase-rp_device_profile_info-infos \
$libPath/mob-data-export-1.0.0.jar \
"{
    \"operationName\": \"hiveToHbase-rp_device_profile_info-infos\",
    \"sourceInfo\": {
        \"object\": \"hive\",
        \"rowkeyField\": \"device\",
        \"database\": \"rp_sdk_dmp\",
        \"table\": \"rp_device_profile_incr\",
        \"partition\": \"day=${DATE}\",
        \"fields\": [
            \"gender as cf:gender\",
            \"agebin as cf:agebin\",
            \"segment as cf:segment\",
            \"edu as cf:edu\",
            \"kids as cf:kids\",
            \"income as cf:income\",
            \"model_level as cf:model_level\",
            \"tot_install_apps as cf:tot_install_apps\",
            \"tag_list as cf:tags\",
            \"model as cf:model\",
            \"carrier as cf:carrier\",
            \"network as cf:network\",
            \"screensize as cf:screensize\",
            \"sysver as cf:sysver\",
            \"city_level as cf:city_level\",
            \"permanent_country as cf:permanent_country\",
            \"permanent_province as cf:permanent_province\",
            \"permanent_city as cf:permanent_city\",
            \"occupation as cf:occupation\",
            \"house as cf:house\",
            \"repayment as cf:repayment\",
            \"car as cf:car\",
            \"workplace as cf:workplace\",
            \"residence as cf:residence\",
			 \"country as cf:country\",
            \"province as cf:province\",
            \"city as cf:city\",
            \"married as cf:married\"
        ]
    },
    \"targetInfo\": {
        \"object\": \"hbase\",
        \"table\": \"rp_device_profile_info\",
        \"zkQuorum\": \"10.6.160.98,10.6.160.99,10.6.160.107\"
    }
}
"

: '
实现功能：
        将rp_sdk_dmp.imei_device_tag_codis_increment 表中的增量数据导入到Hbase表rp_device_profile_info中
实现逻辑：
        将下列字段插入到hbase中
输出字段
device                          --设备号
country                         --国家
province                        --省
city                            --城市
imei                            --imei号
cell_factory                    --厂商
'
/usr/bin/spark-submit \
--class com.mob.job.DataExport \
--jars /opt/cloudera/parcels/CDH-5.7.6-1.cdh5.7.6.p0.6/jars/hbase-spark-1.2.0-cdh5.7.6.jar \
--queue dmpots \
--master yarn-cluster \
--driver-memory 10G \
--num-executors 5 \
--executor-memory 10G \
--executor-cores 6 \
--conf spark.yarn.executor.memoryOverhead=4096 \
--conf spark.app.name=DataExport-tohbase-rp_device_profile_info-imei \
$libPath/mob-data-export-1.0.0.jar \
"{
    \"operationName\": \"hiveToHbase-rp_device_profile_info-imei\",
    \"sourceInfo\": {
        \"object\": \"hive\",
        \"rowkeyField\": \"device\",
        \"database\": \"rp_sdk_dmp\",
        \"table\": \"imei_device_tag_codis_increment\",
        \"partition\": \"dt=${DATE}\",
        \"fields\": [
            \"imei as cf:imei\",
            \"cell_factory as cf:cell_factory\"
        ]
    },
    \"targetInfo\": {
        \"object\": \"hbase\",
        \"table\": \"rp_device_profile_info\",
        \"zkQuorum\": \"10.6.160.98,10.6.160.99,10.6.160.107\"
    }
}
"

#以下是199集群
: '
实现功能：
        将rp_sdk_dmp.rp_device_profile_incr 表中的增量数据导入到Hbase表rp_device_profile_info中
实现逻辑：
        将下列字段插入到hbase中
输出字段
device                          --设备号
gender                          --性别
agebin                          --年龄
segment                         --人物标签
edu                             --教育
kids                            --有无孩子
income                          --收入
model_level                     --设备级别
tot_install_apps                --总装量
tags                            --标签
model                           --设备型号
carrier                         --运营商代码
network                         --网络
screensize                      --分辨率
sysver                          --操作系统
city_level                      --城市等级(1 - 一线城市;  2 - 二线城市;   3 - 三线城市;    4 - 四线城市;    5 - 五线城市;    -1 - 未知)
permanent_country               --常驻国家
permanent_province              --常驻省份
permanent_city                  --常驻城市
occupation                      --职业
house                           --房产情况
repayment                       --偿还能力
car                             --车产情况
workplace                       --工作地
residence                       --居住地
married                         --婚姻状况
'
/usr/bin/spark-submit \
--class com.mob.job.DataExport \
--jars /opt/cloudera/parcels/CDH-5.7.6-1.cdh5.7.6.p0.6/jars/hbase-spark-1.2.0-cdh5.7.6.jar \
--queue dmpots \
--master yarn-cluster \
--driver-memory 10G \
--num-executors 5 \
--executor-memory 10G \
--executor-cores 6 \
--conf spark.yarn.executor.memoryOverhead=10240 \
--conf spark.app.name=DataExport-tohbase-rp_device_profile_info-infos \
$libPath/mob-data-export-1.0.0.jar \
"{
    \"operationName\": \"hiveToHbase-rp_device_profile_info-infos\",
    \"sourceInfo\": {
        \"object\": \"hive\",
        \"rowkeyField\": \"device\",
        \"database\": \"rp_sdk_dmp\",
        \"table\": \"rp_device_profile_incr\",
        \"partition\": \"day=${DATE}\",
        \"fields\": [
            \"gender as cf:gender\",
            \"agebin as cf:agebin\",
            \"segment as cf:segment\",
            \"edu as cf:edu\",
            \"kids as cf:kids\",
            \"income as cf:income\",
            \"model_level as cf:model_level\",
            \"tot_install_apps as cf:tot_install_apps\",
            \"tag_list as cf:tags\",
            \"model as cf:model\",
            \"carrier as cf:carrier\",
            \"network as cf:network\",
            \"screensize as cf:screensize\",
            \"sysver as cf:sysver\",
            \"city_level as cf:city_level\",
            \"permanent_country as cf:permanent_country\",
            \"permanent_province as cf:permanent_province\",
            \"permanent_city as cf:permanent_city\",
            \"occupation as cf:occupation\",
            \"house as cf:house\",
            \"repayment as cf:repayment\",
            \"car as cf:car\",
            \"workplace as cf:workplace\",
            \"residence as cf:residence\",
			\"country as cf:country\",
            \"province as cf:province\",
            \"city as cf:city\",
            \"married as cf:married\"
        ]
    },
    \"targetInfo\": {
        \"object\": \"hbase\",
        \"table\": \"rp_device_profile_info\",
        \"zkQuorum\": \"10.6.28.101,10.6.25.199,10.6.25.200\"
    }
}
"

: '
实现功能：
        将rp_sdk_dmp.imei_device_tag_codis_increment 表中的增量数据导入到Hbase表rp_device_profile_info中
实现逻辑：
        将下列字段插入到hbase中
输出字段
device                          --设备号
country                         --国家
province                        --省
city                            --城市
imei                            --imei号
cell_factory                    --厂商
'
/usr/bin/spark-submit \
--class com.mob.job.DataExport \
--jars /opt/cloudera/parcels/CDH-5.7.6-1.cdh5.7.6.p0.6/jars/hbase-spark-1.2.0-cdh5.7.6.jar \
--queue dmpots \
--master yarn-cluster \
--driver-memory 10G \
--num-executors 5 \
--executor-memory 10G \
--executor-cores 6 \
--conf spark.yarn.executor.memoryOverhead=4096 \
--conf spark.app.name=DataExport-tohbase-rp_device_profile_info-imei \
$libPath/mob-data-export-1.0.0.jar \
"{
    \"operationName\": \"hiveToHbase-rp_device_profile_info-imei\",
    \"sourceInfo\": {
        \"object\": \"hive\",
        \"rowkeyField\": \"device\",
        \"database\": \"rp_sdk_dmp\",
        \"table\": \"imei_device_tag_codis_increment\",
        \"partition\": \"dt=${DATE}\",
        \"fields\": [
            \"imei as cf:imei\",
            \"cell_factory as cf:cell_factory\"
        ]
    },
    \"targetInfo\": {
        \"object\": \"hbase\",
        \"table\": \"rp_device_profile_info\",
        \"zkQuorum\": \"10.6.28.101,10.6.25.199,10.6.25.200\"
    }
}
"
