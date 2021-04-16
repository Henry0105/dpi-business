CREATE TABLE IF NOT EXISTS `rp_dataengine.mobeye_o2o_lbs_frequency_daily`(
  `storeid` string,
  `device` string,
  `gender` int,
  `agebin` int,
  `segment` int,
  `edu` int,
  `kids` int,
  `income` int,
  `occupation` string,
  `house` int,
  `repayment` int,
  `car` int,
  `married` int,
  `frequency` struct<lat:string,lon:string,province:string,city:string,area:string>,
  `geohash_center5` struct<lat:string,lon:string>,
  `geohash_center6` struct<lat:string,lon:string>,
  `geohash_center7` struct<lat:string,lon:string>,
  `geohash_center8` struct<lat:string,lon:string>,
  `type_1` struct<lat:string,lon:string,name:string>,
  `type_2` struct<lat:string,lon:string,name:string>,
  `type_3` struct<lat:string,lon:string,name:string>,
  `type_4` struct<lat:string,lon:string,name:string>,
  `type_5` struct<lat:string,lon:string,name:string>,
  `type_6` struct<lat:string,lon:string,name:string>,
  `type_7` struct<lat:string,lon:string,name:string>,
  `type_8` struct<lat:string,lon:string,name:string>,
  `type_9` struct<lat:string,lon:string,name:string>,
  `type_10` struct<lat:string,lon:string,name:string>)
PARTITIONED BY (
  `rank_date` string,
  `userid` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
;