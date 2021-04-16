CREATE TABLE `dm_mobdi_master.device_active_applist`
(
    `device` string COMMENT '设备id',
    `plat`   int COMMENT '平台',
    `pkg`    string COMMENT '包名',
    `apppkg` string COMMENT '清洗后的包名',
    `source` string COMMENT '来源'
)
    COMMENT '设备活跃app包,整合所有活跃信息表pv,runtimes,logrun，xm_runtimes包括ios和安卓，c只取clienttime是当日或前一日的数据'
    PARTITIONED BY (
        `day` string COMMENT '日期')
    ROW FORMAT SERDE
        'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
    STORED AS INPUTFORMAT
        'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
        OUTPUTFORMAT
            'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'