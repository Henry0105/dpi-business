#!/usr/bin/env bash
: '
@owner:menff
@describe: sdk+根据卸载预测模型预测数据
@projectName:dataEngine
@BusinessName:unstall_pred
@SourceTable:dm_sdk_master.device_install_app_master,dm_sdk_master.device_sdk_run_master,rp_sdk_dmp.app_active_weekly,rp_sdk_dmp.rp_device_active_label_profile,dw_ext_crawl.phone_model_info_zgc,dm_sdk_mapping.city_level_mapping,rp_sdk_dmp.rp_device_profile_full, rp_sdkplus_uninstall.model_device_top_app4000,dm_sdk_master.master_reserved
@TargetTable:rp_sdkplus_uninstall.device_uninstall_predicting_tmp,rp_sdkplus_uninstall.device_sdk_run_master_2weeks,rp_sdkplus_uninstall.device_uninstall_predict_features,rp_sdkplus_uninstall.device_uninstall_prediction,rp_sdkplus_uninstall.device_uninstall_prediction_stat
@TableRelation:dm_sdk_master.device_install_app_master,dm_sdk_master.device_sdk_run_master->rp_sdkplus_uninstall.device_uninstall_predicting_tmp|dm_sdk_master.device_sdk_run_master->rp_sdkplus_uninstall.device_sdk_run_master_2weeks|rp_sdkplus_uninstall.device_uninstall_predicting_tmp,rp_sdkplus_uninstall.device_sdk_run_master_2weeks,rp_sdk_dmp.app_active_weekly,rp_sdk_dmp.rp_device_active_label_profile,dw_ext_crawl.phone_model_info_zgc,dm_sdk_mapping.city_level_mapping,rp_sdk_dmp.rp_device_profile_full, rp_sdkplus_uninstall.model_device_top_app4000,dm_sdk_master.master_reserved->rp_sdkplus_uninstall.device_uninstall_predict_features|rp_sdkplus_uninstall.device_uninstall_predict_features->rp_sdkplus_uninstall.device_uninstall_prediction|rp_sdkplus_uninstall.device_uninstall_prediction->rp_sdkplus_uninstall.device_uninstall_prediction_stat
'

: '
1. 从@dm_sdk_master.device_install_app_master, @dm_sdk_master.device_sdk_run_master, 获取要预测的设备,并将其写入@rp_sdkplus_uninstall.device_uninstall_predicting_tmp
2. 从
@rp_sdkplus_uninstall.device_uninstall_predicting_tmp
@rp_sdkplus_uninstall.device_sdk_run_master_2weeks
@rp_sdk_dmp.app_active_weekly
@rp_sdk_dmp.rp_device_active_label_profile
@dw_ext_crawl.phone_model_info_zgc
@dm_sdk_mapping.city_level_mapping
@rp_sdk_dmp.rp_device_profile_full
@dm_sdk_master.master_reserved
@rp_sdkplus_uninstall.model_device_top_app4000
    去过滤一堆数据生成特征存入rp_sdkplus_uninstall.device_uninstall_predict_features
3. 对特征数据进行预测存入@rp_sdkplus_uninstall.device_uninstall_prediction
'

set -e -x

# 传入3个参数,日期
# 20180511 20180511 false

if [ $# -lt 3 ]; then
    echo "ERROR: wrong number of parameters"
    echo "USAGE: <day> <modelDay> <refresh2weeks>"
    exit 1
fi
cd `dirname $0`
day=$1
modelDay=$2
refresh2weeks=$3
modelPathRoot="/dmgroup/dba/modelpath/linear_regression_model/uninstall_predict"

: '
0. 配置环境变量
'
source ./uninstall_env.sh

chiSeqKey='table_model_device_top_app4000'
tmp=`cat $conf_dir/hive_database_table.properties | grep "$chiSeqKey"` # 存放每个模型卡方选择出来的pkg
modelPkgsTable="${tmp:${#chiSeqKey}+1}"

: '
1. 预测部分
'

# 1. 准备特征
/opt/mobdata/sbin/spark-submit --master yarn --deploy-mode cluster \
    --class com.mob.dataengine.predict.uninstall.DeviceUninstallBuilder  \
    --driver-memory 6g --executor-memory 10g --executor-cores 2 \
    --name DeviceUninstallBuilder_$day \
    --conf spark.memory.useLegacyMode=true \
    --conf spark.executorEnv.JAVA_HOME=/opt/jdk1.8.0_45 \
    --conf spark.yarn.appMasterEnv.JAVA_HOME=/opt/jdk1.8.0_45 \
    --conf spark.storage.memoryFraction=0.1 \
    --conf spark.shuffle.memoryFraction=0.7 \
    --conf "spark.executor.extraJavaOptions=-XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35 -XX:ConcGCThreads=12" \
    --conf spark.dynamicAllocation.enabled=true \
    --conf spark.dynamicAllocation.minExecutors=5 \
    --conf spark.dynamicAllocation.maxExecutors=50 \
    --conf spark.shuffle.service.enabled=true \
    --conf spark.kryoserializer.buffer.max=512m \
    --conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:log4j.properties -XX:PermSize=512m" \
    --files "$conf_dir/log4j.properties","$conf_dir/hive_database_table.properties" \
    --jars $lib_dir/jars/scopt_2.11-3.3.0.jar \
    --verbose \
    $job_jar \
    --version 1000.20180515 \
    --currDay $day \
    --modelDay $modelDay \
    --force false \
    --refresh2weeks $refresh2weeks

# 2. score部分
# 根据modelDay获取appkey,循环调用score代码

models=`hive -S -e "show partitions $modelPkgsTable" | grep "$modelDay"`
readarray -t array <<<"$models"
for i in "${!array[@]}"
do
    s="${array[i]}"
    appkey="${s:7:${#s}-20}"
    echo "$appkey"

    modelPath="$modelPathRoot/$modelDay/$appkey"
    echo "$modelPath"

    /opt/mobdata/sbin/spark-submit --master yarn --deploy-mode cluster \
    --class com.mob.dataengine.predict.uninstall.DeviceUninstallScore \
    --executor-memory 5g --driver-memory 5g --executor-cores 4 \
    --name DeviceUninstallScore_$day \
    --conf "spark.executor.extraJavaOptions=-XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35 -XX:ConcGCThreads=12" \
    --conf spark.dynamicAllocation.enabled=true \
    --conf spark.dynamicAllocation.minExecutors=5 \
    --conf spark.dynamicAllocation.maxExecutors=50 \
    --conf spark.executorEnv.JAVA_HOME=/opt/jdk1.8.0_45 \
    --conf spark.yarn.appMasterEnv.JAVA_HOME=/opt/jdk1.8.0_45 \
    --conf spark.shuffle.service.enabled=true \
    --conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:log4j.properties -XX:PermSize=512m" \
    --files "$conf_dir/log4j.properties","$conf_dir/hive_database_table.properties" \
    --jars $lib_dir/jars/scopt_2.11-3.3.0.jar \
    --verbose \
    $job_jar \
    --appKeys $appkey \
    --currDay $day \
    --modelDay $modelDay \
    --modelPath $modelPath
done



: '
2. 将预测的数据推到服务端
2.1 预测统计结果写入mysql
2.2 预测详细device信息写入es
'

# 1. 预测统计结果写入mysql
query="select concat_ws('-', array(substring(day, 0, 4), substring(day, 5,2), substring(day, 7,2))) day, appkey,
stat['90'] percent_90, stat['70'] percent_70,stat['60'] percent_60,
stat['50'] percent_50, stat['40'] percent_40, stat['30'] percent_30,stat['20'] percent_20
from (
    select day, appkey, stat from (
        select predict_target_day as day, app_key as appkey, stat, process_time, row_number() over (partition by app_key order by process_time desc) rank
        from $table_device_uninstall_prediction_stat
        where predict_target_day=$day
    ) as a
    where rank = 1
) as b
"

mysql_info="{
    \"user\": \"${uninstall_mysql_user}\",
    \"pwd\": \"${uninstall_mysql_pwd}\",
    \"host\": \"${uninstall_mysql_host}\",
    \"port\": ${uninstall_mysql_port},
    \"db\": \"sdk_uninstall_forecast\",
    \"table\": \"appkey_uninstall_forecast_daily\",
    \"fields\": [\"day\", \"appkey\", \"percent_90\",\"percent_70\",\"percent_60\",\"percent_50\",\"percent_40\",\"percent_30\",\"percent_20\"]
}
"

/opt/mobdata/sbin/spark-submit --master yarn \
            --deploy-mode cluster \
            --class com.mob.dataengine.predict.uninstall.Export2Mysql  \
            --name "appkey_uninstall_predict_daily_$day" \
            --conf spark.dynamicAllocation.enabled=true \
            --conf spark.dynamicAllocation.minExecutors=1 \
            --conf spark.shuffle.service.enabled=true \
            --conf spark.dynamicAllocation.maxExecutors=2 \
            --conf spark.locality.wait=100ms \
            --conf spark.executorEnv.JAVA_HOME=/opt/jdk1.8.0_45 \
            --conf spark.yarn.appMasterEnv.JAVA_HOME=/opt/jdk1.8.0_45 \
            --conf spark.sql.codegen=true \
            --conf spark.sql.broadcastTimeout=600 \
            --conf spark.sql.orc.filterPushdown=true \
            --conf spark.shuffle.io.maxRetries=6 \
            --conf spark.shuffle.io.retryWait=10s \
            --conf spark.network.timeout=300000 \
            --conf spark.core.connection.ack.wait.timeout=300000 \
            --conf spark.storage.blockManagerSlaveTimeoutMs=300000 \
            --conf spark.shuffle.io.connectionTimeout=300000 \
            --conf spark.rpc.askTimeout=3000 \
            --conf spark.rpc.lookupTimeout=300000 \
            --conf "spark.driver.extraJavaOptions=-XX:PermSize=512m" \
            --verbose \
            --executor-memory 12g \
            --executor-cores 4 \
            --jars $lib_dir/mysql-connector-java-5.1.29.jar \
            $job_jar "$query" "$mysql_info"

# 2. 将score的数据分bin写入csv格式的hive表并join,imei,画像信息,gender,agebin,cityLevel,income,sysver,modelLevel

hive -v -e "
set hive.vectorized.execution.enabled=true;
set hive.vectorized.execution.reduce.enabled=true;
set hive.exec.parallel=true;

set mapred.max.split.size=256000000;
set mapred.min.split.size.per.node=100000000;
set mapred.min.split.size.per.rack=100000000;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.merge.smallfiles.avgsize=256000000;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.merge.size.per.task = 256000000;
set hive.hadoop.supports.splittable.combineinputformat=true;
set hive.exec.dynamic.partition.mode=nonstrict;

insert overwrite table $table_score_uninstall_bin partition(day=$day)
select b.device, flag_map,
    map('imei', d.imei, 'gender', gender, 'agebin', agebin, 
        'city_level', city_level, 'income', income, 'sysver', sysver, 'model_level', model_level)
from (
    select device, str_to_map(concat_ws(',', collect_list(concat(appkey, ':', flag)))) as flag_map
    from (
        select
            device,    
            CASE 
                WHEN probability >= 0.9 THEN '0.9'
                WHEN probability >= 0.7 THEN '0.7'
                WHEN probability >= 0.6 THEN '0.6'
                WHEN probability >= 0.5 THEN '0.5'
            end as flag,
            appkey
        from $table_device_uninstall_prediction
        where day=$day and probability >= 0.5
    ) as a
    group by device
) as b
left join (
    select device, gender, agebin, if(city_level>=5, 5, city_level) as city_level, income, sysver, model_level
    from $table_device_uninstall_predict_features
    where day=$day
) as c
on b.device = c.device
left join (select device, imei from $table_dm_imei_latest_tags_mapping_view) as d
on b.device = d.device;
"

# 3. 将rp_sdkplus_uninstall.score_uninstall_bin的数据从导入es

for i in "${!array[@]}"
do
    s="${array[i]}"
    appkey="${s:7:${#s}-20}"
    echo "$appkey"

    esUrl="${uninstall_es_path}/esimporter/uninstall?appkey=$appkey"

    query="
    select device as deviceid,
        flag_map['$appkey'] as score,
        tag_map['imei'] as imei,
        cast(tag_map['gender'] as int) as gender,
        cast(tag_map['agebin'] as int) as agebin,
        cast(tag_map['city_level'] as int) as cityLevel,
        cast(tag_map['income'] as int) as income,
        tag_map['sysver'] as sysver,
        cast(tag_map['model_level'] as int) as modelLevel
    from $table_score_uninstall_bin
    where day=$day and length(flag_map['$appkey']) > 0
    "

    spark-submit \
    --class com.mob.job.DataExport \
    --master yarn-cluster \
    --name "uninstall_predict2es_$day" \
    --driver-memory 5G \
    --num-executors 10 \
    --executor-memory 2G \
    --executor-cores 2 \
    --verbose \
    --conf "spark.driver.extraJavaOptions=-XX:PermSize=512m" \
    --conf spark.app.name=DataExport-totestes \
    $lib_dir/mob-data-export-2.0.3.jar \
    "{
        \"operationName\": \"hiveToEs\",
        \"sourceInfo\": {
            \"object\": \"hive\",
            \"database\": \"rp_sdkplus_uninstall\",
            \"table\": \"score_uninstall_bin\",
            \"partition\": \"day=$day\",
            \"fields\": [
                \"deviceid as deviceid\",
                \"score as score\",
                \"imei as imei\",
                \"gender as gender\",
                \"agebin as agebin\",
                \"cityLevel as cityLevel\",
                \"income as income\",
                \"sysver as sysver\",
                \"modelLevel as modelLevel\"
            ],
            \"sql\": \"$query\"
        },
        \"targetInfo\": {
            \"object\": \"es\",
            \"operateType\": \"http\",
            \"valueType\": \"jsonArray\",
            \"esUrl\": \"$esUrl\",
            \"batchSize\": \"20000\"
        }
    }
    "
done