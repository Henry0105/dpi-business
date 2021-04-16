CREATE TABLE IF NOT EXISTS `dm_sdk_mapping.apppkg_info`(
  `apppkg` string COMMENT 'app清理后的包名',
  `icon` string COMMENT 'app图标链接地址',
  `name` string COMMENT 'app名称',
  `cate_id` string COMMENT '一级分类id',
  `cate_name` string COMMENT '一级分类名称',
  `cate_l2_id` string COMMENT '二级分类id',
  `cate_l2` string COMMENT '二级分类名称')
COMMENT 'app信息'
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
;