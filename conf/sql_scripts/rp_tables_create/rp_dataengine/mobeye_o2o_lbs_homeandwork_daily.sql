CREATE TABLE IF NOT EXISTS `rp_dataengine.mobeye_o2o_lbs_homeandwork_daily`(
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
  `home` struct<lat:string,lon:string,province:string,city:string,area:string>,
  `home_type` struct<lat:string,lon:string,name:string>,
  `home_geohash_center5` struct<lat:string,lon:string>,
  `home_geohash_center6` struct<lat:string,lon:string>,
  `home_geohash_center7` struct<lat:string,lon:string>,
  `home_geohash_center8` struct<lat:string,lon:string>,
  `work` struct<lat:string,lon:string,province:string,city:string,area:string>,
  `work_type` struct<lat:string,lon:string,name:string>,
  `work_geohash_center5` struct<lat:string,lon:string>,
  `work_geohash_center6` struct<lat:string,lon:string>,
  `work_geohash_center7` struct<lat:string,lon:string>,
  `work_geohash_center8` struct<lat:string,lon:string>)
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