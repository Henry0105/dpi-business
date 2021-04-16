CREATE TABLE `dm_sdk_mapping.pkg_name_mapping`
(
    `pkg`        string COMMENT 'app包名',
    `name`       string COMMENT 'app名称',
    `cnt`        bigint COMMENT '计数',
    `update_day` string COMMENT '更新时间'
)
    COMMENT 'app包信息'
    ROW FORMAT SERDE
        'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
    STORED AS INPUTFORMAT
        'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
        OUTPUTFORMAT
            'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'