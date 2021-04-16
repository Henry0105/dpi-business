CREATE TABLE IF NOT EXISTS `rp_mobdi_app.rp_device_outing`(
  `device` string COMMENT '设备id',
  `out_country` array<string> COMMENT '出境国家列表',
  `tot_times` int COMMENT '出境次数，',
  `per_country` string COMMENT '常住地国家',
  `plat` int COMMENT '手机系统平台：1安卓，2ios',
  `tot_times_year` int COMMENT '一年出境次数')
COMMENT '设备处境信息'
PARTITIONED BY (
  `day` string COMMENT '日期',
  `time_window` string COMMENT '时间窗口')
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
;