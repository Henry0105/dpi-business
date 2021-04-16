CREATE TABLE IF NOT EXISTS `rp_dataengine.mobeye_o2o_base_score_daily`(
  `storeid` string,
  `type` string,
  `sub_type` string,
  `percent` double)
PARTITIONED BY (
  `day` string,
  `userid` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat';

CREATE TABLE IF NOT EXISTS `mobeye_o2o_base_score_weekly`(
  `storeid` string,
  `type` string,
  `sub_type` string,
  `percent` double)
PARTITIONED BY (
  `day` string,
  `userid` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat';


CREATE TABLE IF NOT EXISTS `mobeye_o2o_base_score_monthly`(
  `storeid` string,
  `type` string,
  `sub_type` string,
  `percent` double)
PARTITIONED BY (
  `day` string,
  `userid` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat';
