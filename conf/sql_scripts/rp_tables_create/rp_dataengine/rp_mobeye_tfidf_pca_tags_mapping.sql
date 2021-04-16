CREATE TABLE IF NOT EXISTS `rp_dataengine.rp_mobeye_tfidf_pca_tags_mapping`(
  `device` string,
  `imei` string,
  `mac` string,
  `phone` string,
  `tfidflist` string,
  `country` string,
  `province` string,
  `city` string,
  `gender` string,
  `agebin` string,
  `segment` string,
  `edu` string,
  `kids` string,
  `income` string,
  `cell_factory` string,
  `model` string,
  `model_level` string,
  `carrier` string,
  `network` string,
  `screensize` string,
  `sysver` string,
  `occupation` string,
  `house` string,
  `repayment` string,
  `car` string,
  `workplace` string,
  `residence` string,
  `applist` string,
  `married` string,
  `match_flag` string,
  `processtime` string)
COMMENT 'looklike的数据准备
从rp_device_profile_full取出上次更新后的数据，做pca降维处理'
PARTITIONED BY (
  `day` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'