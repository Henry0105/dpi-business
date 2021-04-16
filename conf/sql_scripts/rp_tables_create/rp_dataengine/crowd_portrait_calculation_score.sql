-- departed
CREATE TABLE IF NOT EXISTS `rp_dataengine.crowd_portrait_calculation_score`(
  `label` string COMMENT '标签id',
  `label_id` string COMMENT '子类别',
  `cnt` double COMMENT '计数',
  `sum` double COMMENT '求和')
COMMENT '自定义群体标签计算结果表'
PARTITIONED BY (
  `uuid` string COMMENT 'UUID 分区字段'
  ) STORED AS orc;