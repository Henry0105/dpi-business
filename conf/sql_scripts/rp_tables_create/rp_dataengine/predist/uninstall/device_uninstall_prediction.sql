CREATE TABLE IF NOT EXISTS `rp_sdkplus_uninstall.device_uninstall_prediction`(
  `device` string,
  `probability` double COMMENT '可能性',
  `prediction` int COMMENT '预测'
)
PARTITIONED BY (
  `appkey` string,
  `model_day` string,
  `day` string
  )
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat';