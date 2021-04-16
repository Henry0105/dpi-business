CREATE TABLE `dm_sdk_mapping.rp_app_name_info`
(
    `apppkg`   string COMMENT 'app包名',
    `app_name` string COMMENT 'app名称',
    `cnt`      bigint COMMENT '计数',
    `day`      string COMMENT '日期'
)
COMMENT 'app包信息，包名清洗的一定在这个里面，但是这个里面的包名不全是清洗过的'
ROW FORMAT SERDE
    'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
    'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
    'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'