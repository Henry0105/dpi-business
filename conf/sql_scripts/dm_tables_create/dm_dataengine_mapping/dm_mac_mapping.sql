CREATE TABLE `dm_dataengine_mapping.dm_mac_mapping`(
  `mac_md5` string COMMENT 'mac的md5值',
  `mac` string COMMENT 'mac',
  `device_plat` array<string> COMMENT '设备id的平台(android=1,ios=2)',
  `device` array<string> COMMENT '设备id',
  `device_tm` array<string> COMMENT '设备id的更新时间',
  `duid` array<string> COMMENT 'duid',
  `update_time` string COMMENT '最新时间',
  `imei` array<string> COMMENT 'imei',
  `imei_tm` array<string> COMMENT 'imei更新时间',
  `phone` array<string> COMMENT 'phone',
  `phone_tm` array<string> COMMENT 'phone更新时间')
COMMENT 'mac=>device映射表'
PARTITIONED BY (
  `day` string COMMENT '创建时间')
stored as orc;
CREATE TABLE `dm_dataengine_mapping.dm_mac_mapping_v2`(
  `mac_md5` string COMMENT 'mac的md5值',
  `mac` string COMMENT 'mac',
  `device_plat` array<string> COMMENT '设备id的平台(android=1,ios=2)',
  `device` array<string> COMMENT '设备id',
  `device_tm` array<string> COMMENT '设备id的更新时间',
  `duid` array<string> COMMENT 'duid',
  `update_time` string COMMENT '最新时间',
  `imei` array<string> COMMENT 'imei',
  `imei_tm` array<string> COMMENT 'imei更新时间',
  `phone` array<string> COMMENT 'phone',
  `phone_tm` array<string> COMMENT 'phone更新时间',
  `device_ltm` array<string> COMMENT '设备id的最后活跃时间',
  `imei_ltm` array<string> COMMENT 'imei的最后活跃时间',
  `phone_ltm` array<string> COMMENT 'phone更新时间'
  )
COMMENT 'mac=>device映射表'
PARTITIONED BY (
  `day` string COMMENT '创建时间')
STORED AS orc;
CREATE TABLE `dm_dataengine_mapping.dm_mac_mapping_v3`(
  `mac_md5` string COMMENT 'mac的md5值',
  `mac` string COMMENT 'mac',
  `device_plat` array<string> COMMENT '设备id的平台(android=1,ios=2)',
  `device` array<string> COMMENT '设备id',
  `device_tm` array<string> COMMENT '设备id的更新时间',
  `duid` array<string> COMMENT 'duid',
  `update_time` string COMMENT '最新时间',
  `imei` array<string> COMMENT 'imei',
  `imei_md5` array<string>,
  `imei_tm` array<string> COMMENT 'imei更新时间',
  `imei_14` array<string>,
  `imei_14_md5` array<string>,
  `imei_14_tm` array<string>,
  `imei_14_ltm` array<string>,
  `imei_15` array<string>,
  `imei_15_md5` array<string>,
  `imei_15_tm` array<string>,
  `imei_15_ltm` array<string>,
  `phone` array<string> COMMENT 'phone',
  `phone_md5` array<string>,
  `phone_tm` array<string> COMMENT 'phone更新时间',
  `device_ltm` array<string> COMMENT '设备id的最后活跃时间',
  `imei_ltm` array<string> COMMENT 'imei的最后活跃时间',
  `phone_ltm` array<string> COMMENT 'phone更新时间',
  `imsi` array<string>,
  `imsi_md5` array<string>,
  `imsi_tm` array<string>,
  `imsi_ltm` array<string>,
  `serialno` array<string>,
  `serialno_tm` array<string>,
  `serialno_ltm` array<string>,
  `idfa` array<string>,
  `idfa_tm` array<string>,
  `idfa_ltm` array<string>,
  `orig_imei` array<string> COMMENT 'orig_imei',
  `orig_imei_tm` array<string> COMMENT 'orig_imei_tm',
  `orig_imei_ltm` array<string> COMMENT 'orig_imei_ltm',
  `oaid` array<string> COMMENT 'oaid',
  `oaid_tm` array<string> COMMENT 'oaid_tm',
  `oaid_ltm` array<string> COMMENT 'oaid_ltm'
  )
COMMENT 'mac=>device映射表'
PARTITIONED BY (
  `day` string COMMENT '创建时间')
STORED AS orc;

CREATE TABLE `dm_dataengine_mapping.dm_mac_mapping_v3_view`(
  `mac_md5` string COMMENT 'mac的md5值',
  `mac` string COMMENT 'mac',
  `device_plat` array<string> COMMENT '设备id的平台(android=1,ios=2)',
  `device` array<string> COMMENT '设备id',
  `device_tm` array<string> COMMENT '设备id的更新时间',
  `duid` array<string> COMMENT 'duid',
  `update_time` string COMMENT '最新时间',
  `imei` array<string> COMMENT 'imei',
  `imei_md5` array<string>,
  `imei_tm` array<string> COMMENT 'imei更新时间',
  `imei_14` array<string>,
  `imei_14_md5` array<string>,
  `imei_14_tm` array<string>,
  `imei_14_ltm` array<string>,
  `imei_15` array<string>,
  `imei_15_md5` array<string>,
  `imei_15_tm` array<string>,
  `imei_15_ltm` array<string>,
  `phone` array<string> COMMENT 'phone',
  `phone_md5` array<string>,
  `phone_tm` array<string> COMMENT 'phone更新时间',
  `device_ltm` array<string> COMMENT '设备id的最后活跃时间',
  `imei_ltm` array<string> COMMENT 'imei的最后活跃时间',
  `phone_ltm` array<string> COMMENT 'phone更新时间',
  `imsi` array<string>,
  `imsi_md5` array<string>,
  `imsi_tm` array<string>,
  `imsi_ltm` array<string>,
  `serialno` array<string>,
  `serialno_tm` array<string>,
  `serialno_ltm` array<string>,
  `idfa` array<string>,
  `idfa_tm` array<string>,
  `idfa_ltm` array<string>,
  `orig_imei` array<string> COMMENT 'orig_imei',
  `orig_imei_tm` array<string> COMMENT 'orig_imei_tm',
  `orig_imei_ltm` array<string> COMMENT 'orig_imei_ltm',
  `oaid` array<string> COMMENT 'oaid',
  `oaid_tm` array<string> COMMENT 'oaid_tm',
  `oaid_ltm` array<string> COMMENT 'oaid_ltm'
  )
COMMENT 'mac=>device映射表'
PARTITIONED BY (
  `day` string COMMENT '创建时间')
STORED AS orc;