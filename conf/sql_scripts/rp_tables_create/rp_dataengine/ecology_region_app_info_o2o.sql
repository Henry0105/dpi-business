CREATE TABLE IF NOT EXISTS `ecology_region_app_info_o2o`(
  `country` string,
  `province` string,
  `decile` int,
  `apppkg` string,
  `cnt1` int,
  `cnt2` int,
  `cnt3` int,
  `cnt4` int,
  `icon` string,
  `name` string,
  `cate_id` string,
  `cate_name` string,
  `storeid` string)
PARTITIONED BY (
  `rank_date` string,
  `userid` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat';