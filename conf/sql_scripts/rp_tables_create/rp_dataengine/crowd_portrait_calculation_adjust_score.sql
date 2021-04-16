-- departed
CREATE TABLE IF NOT EXISTS `rp_dataengine.crowd_portrait_calculation_adjust_score`(
  `label` string COMMENT '标签id',
  `label_id` string COMMENT '子类别',
  `android_ratio` double COMMENT '安卓校正比例',
  `android_key_cnt` double COMMENT '安卓key count',
  `android_label_cnt` double COMMENT '安卓所有key count',
  `ios_ratio` double COMMENT 'ios校正比例',
  `ios_key_cnt` double COMMENT 'ioskey count',
  `ios_label_cnt` double COMMENT 'ios所有key count',
  `final_percent` double COMMENT 'key校正占比'
  )
PARTITIONED BY (
  `uuid` string COMMENT '唯一id'
  ) STORED AS orc;