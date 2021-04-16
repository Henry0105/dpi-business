CREATE TABLE IF NOT EXISTS `ecology_result_demo_score_lookalike`(
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
  `match_flag` int)
PARTITIONED BY (
  `day` string,
  `id` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat';


CREATE TABLE IF NOT EXISTS `ecology_region_app_info_lookalike`(
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
  `cate_name` string)
PARTITIONED BY (
  `day` string,
  `id` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat';