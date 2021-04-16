CREATE TABLE `dm_dataengine_mapping.dm_idfa_mapping_v2`(
  `idfa_md5` string COMMENT 'idfa的md5',
  `idfa` string COMMENT 'idfa',
  `device` array<string> COMMENT '设备id',
  `device_tm` array<string> COMMENT '设备id的更新时间',
  `duid` array<string> COMMENT 'duid',
  `update_time` string COMMENT '最新时间',
  `device_ltm` array<string> COMMENT '设备id的更新时间',
  `phone` array<string>,
  `phone_tm` array<string>,
  `phone_ltm` array<string>)
COMMENT 'idfa=>device映射表'
PARTITIONED BY (
  `day` string COMMENT '创建时间',
  `plat` string COMMENT 'android=1,ios=2')
stored as orc;
CREATE TABLE `dm_dataengine_mapping.dm_idfa_mapping_v3`(
  `idfa_md5` string COMMENT 'idfa的md5',
  `idfa` string COMMENT 'idfa',
  `device` array<string> COMMENT '设备id',
  `device_tm` array<string> COMMENT '设备id的更新时间',
  `duid` array<string> COMMENT 'duid',
  `update_time` string COMMENT '最新时间',
  `device_ltm` array<string> COMMENT '设备id的更新时间',
  `phone` array<string>,
  `phone_tm` array<string>,
  `phone_ltm` array<string>,
  `mac` array<string>,
  `mac_tm` array<string>,
  `mac_ltm` array<string>
)
COMMENT 'idfa=>device映射表'
PARTITIONED BY (
  `day` string COMMENT '创建时间',
  `plat` string COMMENT 'android=1,ios=2')
stored as orc;
CREATE TABLE `dm_dataengine_mapping.dm_idfa_mapping_v3_view`(
  `idfa_md5` string COMMENT 'idfa的md5',
  `idfa` string COMMENT 'idfa',
  `device` array<string> COMMENT '设备id',
  `device_tm` array<string> COMMENT '设备id的更新时间',
  `duid` array<string> COMMENT 'duid',
  `update_time` string COMMENT '最新时间',
  `device_ltm` array<string> COMMENT '设备id的更新时间',
  `phone` array<string>,
  `phone_tm` array<string>,
  `phone_ltm` array<string>,
  `mac` array<string>,
  `mac_tm` array<string>,
  `mac_ltm` array<string>
)
COMMENT 'idfa=>device映射表'
PARTITIONED BY (
  `day` string COMMENT '创建时间',
  `plat` string COMMENT 'android=1,ios=2')
stored as orc;