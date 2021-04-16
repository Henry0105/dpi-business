CREATE TABLE IF NOT EXISTS `rp_dataengine.rp_device_profile_info_metadata` (
  `table` string COMMENT '表名称',
  `day` string COMMENT '分区'
)
COMMENT 'rp_device_profile_info,每张tag表的最后更新时间'
PARTITIONED BY (
  `version` string COMMENT '时间戳'
)
STORED AS orc;