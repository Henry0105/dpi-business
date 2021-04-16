#!/usr/bin/env bash
: '
@owner:menff
@describe:sdk+日卸载率统计
@projectName:dataEngine
@BusinessName:unstall_pred
@SourceTable:dm_mobdi_master.master_reserved,dm_mobdi_master.device_install_app_master,rp_sdkplus_uninstall.appkey_pkg_mapping
@TargetTable:rp_sdkplus_uninstall.appkey_device_detail_daily,rp_sdkplus_uninstall.appkey_installed_stat_daily,rp_sdkplus_uninstall.appkey_uninstall_stat_daily
@TableRelation:rp_sdkplus_uninstall.appkey_pkg_mapping_par,dm_mobdi_master.master_reserved->rp_sdkplus_uninstall.appkey_device_detail_daily|dm_mobdi_master.device_install_app_master,rp_sdkplus_uninstall.appkey_pkg_mapping_par->rp_sdkplus_uninstall.appkey_installed_detail_daily|rp_sdkplus_uninstall.appkey_installed_detail_daily->rp_sdkplus_uninstall.appkey_installed_stat_daily|rp_sdkplus_uninstall.appkey_device_detail_daily,rp_sdkplus_uninstall.appkey_installed_stat_daily->rp_sdkplus_uninstall.appkey_uninstall_stat_daily
'

set -e -x

# '12f126f4d902e,d580ad56b4b5,androidv1101'
cd `dirname $0`
day="$1"

: '
0. 配置环境变量
'
source ./uninstall_env.sh

: '
步骤一:
从java那边提供的http接口获取获取appkey和pkg的对应关系(http://admin.utag.mob.com/inner/uninstall/appkey-pkg-mapping)
'

json_body=`curl $uninstall_url`

/opt/mobdata/sbin/spark-submit --master yarn \
            --deploy-mode cluster \
            --class com.mob.dataengine.predict.uninstall.Export2Hive  \
            --name Import2Hive_$day \
            --conf spark.dynamicAllocation.enabled=true \
            --conf spark.dynamicAllocation.minExecutors=1 \
            --conf spark.shuffle.service.enabled=true \
            --conf spark.dynamicAllocation.maxExecutors=2 \
            --conf spark.locality.wait=100ms \
            --conf spark.sql.codegen=true \
            --conf spark.sql.broadcastTimeout=600 \
            --conf spark.sql.shuffle.partitions=800 \
            --conf spark.sql.autoBroadcastJoinThreshold=51457280 \
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
            $job_jar "$json_body" "$day" 'rp_sdkplus_uninstall.appkey_pkg_mapping_par'

: '
步骤二:
统计在装量:
1. 统计appkey下每个device的安装/卸载情况
2. 统计appkey下每个device的在装情况
3. 统计每个appkey的在装统计
4. 统计每个appkey的卸载/在装情况
'

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

create temporary table rp_sdkplus_uninstall.pkg_list AS
select appkey, pkg
from $table_appkey_pkg_mapping_par
lateral view explode(split(pkg_list, ',')) tf as pkg
where day='$day';

insert overwrite table $table_appkey_device_detail_daily PARTITION (appkey, day)
select
    device, 
    if(uninstall > install, 1, 0) uninstall,
    if(uninstall < install, 1, 0) install,
    appkey,
    $day
from (
    select 
        device,
        sum(if(refine_final_flag = -1, 1, 0)) as uninstall, 
        sum(if(refine_final_flag=1, 1, 0)) as install,
        appkey
    from $table_master_reserved_new reserved
    inner join rp_sdkplus_uninstall.pkg_list pkg_list
    on reserved.pkg = pkg_list.pkg
    where day='$day' and refine_final_flag in (-1, 1)
    group by device, appkey, refine_final_flag
) as a
where uninstall != install;


INSERT OVERWRITE TABLE $table_appkey_installed_detail_daily PARTITION(appkey, day)
select device, appkey, $day as day
from $table_device_install_app_master_new install_app
inner join rp_sdkplus_uninstall.pkg_list pkg_list
on install_app.pkg = pkg_list.pkg
where day = '$day' and final_flag <> -1
GROUP by device, appkey;


INSERT OVERWRITE TABLE $table_appkey_installed_stat_daily PARTITION(appkey, day)
select count(1), appkey, $day as day
from $table_appkey_installed_detail_daily
where day = '$day'
group by appkey;


INSERT OVERWRITE TABLE $table_appkey_uninstall_stat_daily PARTITION(appkey, day)
select sum(uninstall) as uninstall, sum(installed) as installed, appkey, day from (
    select
        count(1) as uninstall,
        0 as installed,
        appkey,
        $day as day
    from (
        select
            appkey,
            device
        from $table_appkey_device_detail_daily
        where day='$day' and uninstall > install
    ) as c
    group by appkey

    union all

    select
        0 as uninstall,
        cnt as installed,
        appkey,
        $day as day
    from $table_appkey_installed_stat_daily
    where day='$day'
) as d
group by appkey, day;
"

: '
步骤三:
将hive中的数据聚合起来并写入mysql,供java端使用
'

query="
select concat_ws('-', array(substring(day, 0, 4), substring(day, 5,2), substring(day, 7,2))) day, appkey, cast(uninstall as string) as uninstall, cast(installed as string) as installed
from $table_appkey_uninstall_stat_daily
where day = $day
"

mysql_info="{
    \"user\": \"${uninstall_mysql_user}\",
    \"pwd\": \"${uninstall_mysql_pwd}\",
    \"host\": \"${uninstall_mysql_host}\",
    \"port\": ${uninstall_mysql_port},
    \"db\": \"sdk_uninstall_forecast\",
    \"table\": \"appkey_uninstall_daily\",
    \"fields\": [\"day\", \"appkey\", \"uninstall\", \"installed\"]
}
"

/opt/mobdata/sbin/spark-submit --master yarn \
            --deploy-mode cluster \
            --class com.mob.dataengine.predict.uninstall.Export2Mysql  \
            --name "appkey_uninstall_daily_$day" \
            --conf spark.dynamicAllocation.enabled=true \
            --conf spark.dynamicAllocation.minExecutors=1 \
            --conf spark.shuffle.service.enabled=true \
            --conf spark.dynamicAllocation.maxExecutors=2 \
            --conf spark.locality.wait=100ms \
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
            --verbose \
            --conf "spark.driver.extraJavaOptions=-XX:PermSize=512m" \
            --executor-memory 12g \
            --executor-cores 4 \
            --jars $lib_dir/mysql-connector-java-5.1.29.jar \
            $job_jar "$query" "$mysql_info"