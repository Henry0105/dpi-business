CREATE TABLE IF NOT EXISTS `ext_ad_dmp.dw_base_poi_l1_geohash`(
  `g6` string COMMENT 'geohash6编码',
  `g7` string COMMENT 'geohash7编码',
  `lat` string COMMENT '纬度',
  `lon` string COMMENT '经度',
  `name` string COMMENT 'poi店名',
  `type` string COMMENT 'poi类型',
  `type_id` string COMMENT 'poi类型编码')
COMMENT '最新poi数据 带编码'
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'