CREATE TABLE IF NOT EXISTS `rp_mobdi_app.app_active_monthly`(
  `device` string COMMENT '设备ID',
  `apppkg` string COMMENT 'app名称',
  `days` int COMMENT 'app一个月中的活跃天数',
  `day_list` string COMMENT '所有活跃日期的列表，用逗号隔开')
COMMENT '设备的app活跃表(月)'
PARTITIONED BY (
  `month` string COMMENT '月分区')
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat';