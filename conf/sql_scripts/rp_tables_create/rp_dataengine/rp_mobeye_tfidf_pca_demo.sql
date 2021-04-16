CREATE TABLE IF NOT EXISTS `rp_dataengine.rp_mobeye_tfidf_pca_demo`(
  `device` string,
  `tfidflist` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'