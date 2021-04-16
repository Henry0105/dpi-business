-- departed
CREATE TABLE IF NOT EXISTS `rp_dataengine.group_profile_info`(
  `label` string COMMENT '标签id',
  `label_id` string COMMENT '子类别',
  `cnt` INT COMMENT '计数')
COMMENT '群体标签计算结果表'
PARTITIONED BY (
  `day` string COMMENT '日期 分区字段',
  `uuid` string COMMENT 'UUID 分区字段'
  ) STORED AS orc;