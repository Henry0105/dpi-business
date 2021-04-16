CREATE TABLE IF NOT EXISTS `rp_mobdi_app.rp_device_frequency_3monthly`(
  `device` string COMMENT '设备device',
  `lon` double COMMENT '经度',
  `lat` double COMMENT '纬度',
  `cnt` bigint COMMENT '出现天数',
  `country` string COMMENT '国家',
  `province` string COMMENT '省份',
  `city` string COMMENT '城市',
  `area` string COMMENT '区域',
  `rank` int COMMENT '排名')
COMMENT '设备常去地（三个月）'
PARTITIONED BY (
  `day` string COMMENT '时间分区')
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
;