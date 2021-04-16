CREATE TABLE `dm_sdk_mapping.device_duid_mapping_new`(
  `device` string COMMENT '设备id',
  `duid` string COMMENT 'duid',
  `plat` int COMMENT '平台，1->安卓,2->ios',
  `dcookie` string COMMENT 'dcookie',
  `processtime` string COMMENT '这条记录最新一次更新时间')
COMMENT '设备duid mapping表'
