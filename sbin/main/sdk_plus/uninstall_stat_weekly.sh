#!/usr/bin/env bash
: '
@owner:menff
@describe:sdk+周卸载率统计
@projectName:dataEngine
@BusinessName:unstall_pred
@SourceTable:rp_sdkplus_uninstall.appkey_device_detail_daily,rp_sdkplus_uninstall.appkey_installed_stat_daily
@TargetTable:rp_sdkplus_uninstall.appkey_uninstall_stat_weekly
@TableRelation:rp_sdkplus_uninstall.appkey_device_detail_daily,rp_sdkplus_uninstall.appkey_installed_stat_daily->rp_sdkplus_uninstall.appkey_uninstall_stat_weekly
'

set -e -x
cd `dirname $0`
# 10154b13f301a
# appkey="$1"
day="$1"
sat=$(date -d "$day - 6 days" +%Y%m%d)

# 每周日开始跑,传入的天为星期五
if [ $(date -d "$day" +%w) -ne 5 ] ;then
      exit 0
fi

: '
0. 配置环境变量
'
source ./uninstall_env.sh

: '
统计每个appkey的周卸载率
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

INSERT OVERWRITE TABLE $table_appkey_uninstall_stat_weekly PARTITION(appkey, day)
select sum(uninstall) as uninstall, sum(installed) as installed, appkey, $sat as day from (
    select
        count(1) as uninstall,
        0 as installed,
        appkey
    from (
        select
            appkey,
            device,
            sum(uninstall) as uninstall,
            sum(install) as install
        from (
            select
                appkey,
                device,
                uninstall,
                install
            from $table_appkey_device_detail_daily
            where day<=$day and day>=$sat
        ) as c
        group by appkey, device
        having uninstall > install
    ) as d
    group by appkey

    union all

    select
        0 as uninstall,
        cnt as installed,
        appkey
    from $table_appkey_installed_stat_daily
    where day=$sat
) as d
group by appkey;
"

: '
步骤三:
将hive中的数据聚合起来并写入mysql,供java端使用
'
query="
select concat_ws('-', array(substring(day, 0, 4), substring(day, 5,2), substring(day, 7,2))) day, appkey, cast(uninstall as string) as uninstall, cast(installed as string) as installed
from $table_appkey_uninstall_stat_weekly
where day = $sat
"

mysql_info="{
    \"user\": \"${uninstall_mysql_user}\",
    \"pwd\": \"${uninstall_mysql_pwd}\",
    \"host\": \"${uninstall_mysql_host}\",
    \"port\": ${uninstall_mysql_port},
    \"db\": \"sdk_uninstall_forecast\",
    \"table\": \"appkey_uninstall_weekly\",
    \"fields\": [\"day\", \"appkey\", \"uninstall\", \"installed\"]
}
"

/opt/mobdata/sbin/spark-submit --master yarn \
            --deploy-mode cluster \
            --class com.mob.dataengine.predict.uninstall.Export2Mysql  \
            --name "appkey_uninstall_weekly_$day" \
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
            --conf "spark.driver.extraJavaOptions=-XX:PermSize=512m" \
            --verbose \
            --executor-memory 12g \
            --executor-cores 4 \
            --jars $lib_dir/mysql-connector-java-5.1.29.jar \
            $job_jar "$query" "$mysql_info"