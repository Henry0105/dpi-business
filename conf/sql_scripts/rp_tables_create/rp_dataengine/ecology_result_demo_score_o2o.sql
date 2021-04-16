CREATE TABLE IF NOT EXISTS `ecology_result_demo_score_o2o`(
  `deviceid` string,
  `score` double,
  `decile` int,
  `country` string,
  `province` string,
  `city` string,
  `gender` int,
  `agebin` int,
  `segment` int,
  `edu` int,
  `kids` int,
  `income` int,
  `cell_factory` string,
  `model` string,
  `model_level` string,
  `carrier` string,
  `network` string,
  `screensize` string,
  `sysver` string,
  `match_flag` int,
  `permanent_country` string,
  `permanent_province` string,
  `permanent_city` string,
  `occupation` string,
  `house` int,
  `repayment` int,
  `car` int,
  `workplace` string,
  `residence` string,
  `applist` string,
  `married` int,
  `storeid` string,
  `industry` int)
PARTITIONED BY (
  `rank_date` string,
  `userid` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat';