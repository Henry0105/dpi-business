CREATE TABLE IF NOT EXISTS `dm_sdk_mapping.app_category_mapping_par`(
  `pkg` string COMMENT 'app包名(未清理)',
  `apppkg` string COMMENT 'app清理后的包名',
  `appname` string COMMENT 'app名称',
  `cate_l1` string COMMENT 'app一级类别中文',
  `cate_l2` string COMMENT 'app二级类别中文,',
  `cate_l1_id` string COMMENT 'app一级类别id',
  `cate_l2_id` string COMMENT 'app二级类别id')
COMMENT 'app分类信息表'
PARTITIONED BY (
  `version` string COMMENT '版本')
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat';