#!/usr/bin/env bash
: '
@owner:menff
@describe:sdk+总卸载率统计
@projectName:dataEngine
@BusinessName:unstall_pred
@SourceTable:rp_sdkplus_uninstall.appkey_pkg_mapping_par,rp_sdkplus_uninstall.appkey_installed_detail_daily
@TargetTable:rp_sdkplus_uninstall.appkey_uninstall_stat_all,rp_sdkplus_uninstall.appkey_installed_detail_all,rp_sdkplus_uninstall.appkey_uninstall_detail_until
@TableRelation:rp_sdkplus_uninstall.appkey_installed_detail_daily,rp_sdkplus_uninstall.appkey_pkg_mapping_par->rp_sdkplus_uninstall.appkey_installed_detail_all|rp_sdkplus_uninstall.appkey_device_detail_daily,rp_sdkplus_uninstall.appkey_uninstall_detail_until->rp_sdkplus_uninstall.appkey_uninstall_detail_until|rp_sdkplus_uninstall.appkey_uninstall_detail_until,rp_sdkplus_uninstall.appkey_installed_detail_all->rp_sdkplus_uninstall.appkey_uninstall_stat_all
'

set -e -x
cd `dirname $0`
# 传入1个参数
day=$1
bday=$(date -d "$day - 1 day" +%Y%m%d)

: '
0. 配置环境变量
'
source ./uninstall_env.sh

: '
统计总卸载量:
1. 统计每个appkey首次进入卸载统计时的device
2. 统计appkey下每个device到现在为止的安装/卸载情况
3. 统计每个appkey的总卸载率
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

create temporary table rp_sdkplus_uninstall.new_appkey AS
select a.appkey from (
    select appkey
    from $table_appkey_pkg_mapping_par
    where day=$day
) as a
left join (
    select appkey
    from $table_appkey_pkg_mapping_par
    where day=$bday
) as b
on a.appkey = b.appkey
where b.appkey is null;

insert OVERWRITE table $table_appkey_installed_detail_all PARTITION(appkey)
select device, day, a.appkey
from $table_appkey_installed_detail_daily as a
inner join rp_sdkplus_uninstall.new_appkey as b
on a.appkey = b.appkey
where day=$day;

INSERT OVERWRITE TABLE $table_appkey_uninstall_detail_until PARTITION(appkey)
select
    device,
    if(uninstall > install, 1, 0) uninstall,
    if(uninstall < install, 1, 0) install,
    day,
    appkey
from (
    select device, sum(uninstall) as uninstall, sum(install) as install,max(day) as day, appkey from (
        select device, uninstall, install, appkey, day
        from $table_appkey_uninstall_detail_until
        union all
        select device, uninstall, install, appkey, day
        from $table_appkey_device_detail_daily
        where day=$day
    ) as a
    group by device, appkey
) as b
where uninstall != install;


INSERT OVERWRITE TABLE $table_appkey_uninstall_stat_all PARTITION(appkey)
select
    sum(if(uninstall > 0, 1, 0)),
    sum(if(install > 0, 1, 0)),
    appkey
from (
    select appkey, device, sum(uninstall) as uninstall, sum(install) as install from (
        select appkey, device, 0 as uninstall, 1 as install
        from $table_appkey_installed_detail_all
        union all
        select appkey, device, uninstall, install
        from $table_appkey_uninstall_detail_until
    ) as a
    group by appkey, device
) as b
group by appkey;
"


: '
将统计数据写入mysql,格式: key: total_uninstall_percent_$appkey, value: 40%
'

query="
select concat('total_uninstall_percent_', appkey) as name, concat(cast(round(uninstall/installed * 100,1) as string), '%') as value
from $table_appkey_uninstall_stat_all
"

mysql_info="{
    \"user\": \"${uninstall_mysql_user}\",
    \"pwd\": \"${uninstall_mysql_pwd}\",
    \"host\": \"${uninstall_mysql_host}\",
    \"port\": ${uninstall_mysql_port},
    \"db\": \"sdk_uninstall_forecast\",
    \"table\": \"global_value\",
    \"fields\": [\"name\", \"value\"]
}
"

/opt/mobdata/sbin/spark-submit --master yarn \
            --deploy-mode cluster \
            --class com.mob.dataengine.predict.uninstall.Export2Mysql  \
            --name "appkey_uninstall_global_value_$day" \
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
            --executor-memory 12g \
            --executor-cores 4 \
            --conf "spark.driver.extraJavaOptions=-XX:PermSize=512m" \
            --jars $lib_dir/mysql-connector-java-5.1.29.jar \
            $job_jar "$query" "$mysql_info"

echo "uninstall_stat_all to_mysql done"