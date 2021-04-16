CREATE TABLE IF NOT EXISTS `rp_dataengine.appinfo_daily`(
  `storeid` string,
  `icon` string,
  `name` string,
  `cate_name` string,
  `radio` double,
  `index` double,
  `apppkg` string,
  `cate_id` string,
  `cate_l1` string)
COMMENT '区域app信息'
PARTITIONED BY (
  `day` string,
  `userid` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
;

CREATE TABLE IF NOT EXISTS `rp_dataengine.appinfo_daily_v2`(
  `apppkg` string,
  `install_app_device_cnt` int,
  `install_device_cnt` int,
  `active_app_device_cnt` int,
  `active_device_cnt` int,
  `install_penetration` double,
  `install_tgi` double,
  `active_penetration` double,
  `active_tgi` double,
  `appname` string,
  `cate_l1` string,
  `cate_l2` string,
  `icon` string)
COMMENT '媒介画像app信息'
PARTITIONED BY (
  `day` string,
  `uuid` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat';
