CREATE TABLE IF NOT EXISTS `rp_sdkplus_uninstall.device_uninstall_predicting_tmp`(
  `device` string,
  `appkeys` array<string>
)
PARTITIONED BY (
  `day` string,
  `model_day` string
  )
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat';