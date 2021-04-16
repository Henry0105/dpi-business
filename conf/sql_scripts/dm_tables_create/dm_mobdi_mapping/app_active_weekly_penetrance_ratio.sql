CREATE TABLE IF NOT EXISTS `dm_mobdi_mapping.app_active_weekly_penetrance_ratio`(
  `country` string COMMENT '国家',
  `province` string COMMENT '省份',
  `decile` int,
  `apppkg` string COMMENT '清理后的包名',
  `cnt1` bigint COMMENT '每个店铺下面每个app的使用人数',
  `cnt2` bigint COMMENT '每个店铺下所有app的使用人数',
  `cnt3` bigint COMMENT '某个区域下每个app的使用人数',
  `cnt4` bigint COMMENT '某个区域下所有app的使用人数',
  `icon` string COMMENT 'app图标链接地址',
  `name` string COMMENT 'app名称',
  `cate_id` string COMMENT '一级分类id',
  `cate_name` string COMMENT '一级分类名称',
  `cate_l2_id` string COMMENT '二级分类id',
  `cate_l2` string COMMENT '二级分类名称')
COMMENT '计算媒介触达所需要的周表'
PARTITIONED BY (
  `rank_date` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
;