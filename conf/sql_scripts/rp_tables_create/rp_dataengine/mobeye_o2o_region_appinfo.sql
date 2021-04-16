CREATE TABLE IF NOT EXISTS `mobeye_o2o_region_appinfo_weekly`(
  `storeid` string,
  `icon` string,
  `name` string,
  `cate_name` string,
  `radio` double,
  `index` double,
  `apppkg` string,
  `cate_id` string)
PARTITIONED BY (
  `day` string,
  `userid` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat';


CREATE TABLE IF NOT EXISTS `mobeye_o2o_region_appinfo_monthly`(
  `storeid` string,
  `icon` string,
  `name` string,
  `cate_name` string,
  `radio` double,
  `index` double,
  `apppkg` string,
  `cate_id` string)
PARTITIONED BY (
  `day` string,
  `userid` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat';


CREATE TABLE IF NOT EXISTS `mobeye_o2o_region_appinfo_daily`(
  `storeid` string,
  `icon` string,
  `name` string,
  `cate_name` string,
  `radio` double,
  `index` double,
  `apppkg` string,
  `cate_id` string)
PARTITIONED BY (
  `day` string,
  `userid` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat';