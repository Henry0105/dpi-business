CREATE TABLE IF NOT EXISTS `dm_dataengine_mapping.id_mapping_external_full`(
  `owner_data` string COMMENT '交换出去的数据',
  `owner_data_md5` string COMMENT '交换出去的数据 md5加密',
  `ext_data` array<string> COMMENT '反馈的数据 列表',
  `ext_data_md5` array<string> COMMENT '反馈的数据 md5加密 列表',
  `ext_data_tm` array<string> COMMENT '处理时间 列表')
COMMENT '交换数据mapping 全量表'
PARTITIONED BY (
  `day` string COMMENT '时间分区',
  `type` string COMMENT '数据类别')
  stored as orc;

CREATE TABLE IF NOT EXISTS `dm_mobdi_mapping.ext_phone_mapping_incr`(
  `owner_data` string COMMENT '交换出去的数据',
  `ext_data` string COMMENT '反馈的数据 列表',
  `source` int COMMENT '数据源',
  `plat` int COMMENT '1 安卓 2 ios',
  `processtime` string COMMENT '处理时间')
COMMENT '交换数据mapping 增量表'
PARTITIONED BY (
  `day` string COMMENT '时间分区',
  `type` string COMMENT '数据类别')
  stored as orc;

CREATE TABLE IF NOT EXISTS `dm_dataengine_mapping.id_mapping_external_full_view`(
  `owner_data` string COMMENT '交换出去的数据',
  `owner_data_md5` string COMMENT '交换出去的数据 md5加密',
  `ext_data` array<string> COMMENT '反馈的数据 列表',
  `ext_data_md5` array<string> COMMENT '反馈的数据 md5加密 列表',
  `ext_data_tm` array<string> COMMENT '处理时间 列表',
  `type` string)
stored as orc;

CREATE TABLE IF NOT EXISTS `dm_dataengine_mapping.id_mapping_external_full_inc_view`(
  `owner_data` string COMMENT '交换出去的数据',
  `owner_data_md5` string COMMENT '交换出去的数据 md5加密',
  `ext_data` array<string> COMMENT '反馈的数据 列表',
  `ext_data_md5` array<string> COMMENT '反馈的数据 md5加密 列表',
  `ext_data_tm` array<string> COMMENT '处理时间 列表',
  `type` string)
stored as orc;