CREATE TABLE IF NOT EXISTS `rp_mobdi_app.rp_device_location_3monthly`(
  `device` string,
  `lon_home` string,
  `lat_home` string,
  `cluster_home` string,
  `cnt_home` string,
  `max_distance_home` string,
  `min_distance_home` string,
  `confidence_home` string,
  `lon_work` string,
  `lat_work` string,
  `cluster_work` string,
  `cnt_work` string,
  `max_distance_work` string,
  `min_distance_work` string,
  `confidence_work` string,
  `type` int,
  `country_home` string,
  `province_home` string,
  `area_home` string,
  `country_cn_home` string,
  `province_cn_home` string,
  `area_cn_home` string,
  `country_work` string,
  `province_work` string,
  `area_work` string,
  `country_cn_work` string,
  `province_cn_work` string,
  `area_cn_work` string,
  `city_home` string,
  `city_cn_home` string,
  `city_work` string,
  `city_cn_work` string)
PARTITIONED BY (
  `day` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
;