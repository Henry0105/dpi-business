CREATE TABLE IF NOT EXISTS `rp_dataengine.mobeye_o2o_log_clean`(
  `device` string,
  `clientdevice` string,
  `data_type` string,
  `encrypt_type` string,
  `storeid` string,
  `clienttime` string,
  `updatetime` string,
  `processtime` string)
PARTITIONED BY (
  `day` string,
  `userid` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat';