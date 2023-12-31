CREATE TABLE `dm_dataengine_mapping.dm_oaid_mapping_v3`(
  `oaid_md5` string COMMENT 'oaid_md5',
  `oaid` string COMMENT 'oaid',
  `mac`  array<string> COMMENT 'mac',
  `mac_md5`  array<string> COMMENT 'mac_md5',
  `mac_tm`  array<string> COMMENT 'mac_tm',
  `mac_ltm`  array<string> COMMENT 'mac_ltm',
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
  `orig_imei_ltm` array<string> COMMENT 'orig_imei_ltm'
  )
COMMENT 'oaid=>device映射表'
PARTITIONED BY (
  `day` string COMMENT '创建时间',
  `plat` string COMMENT 'android=1,ios=2')
STORED AS orc;
CREATE TABLE `dm_dataengine_mapping.dm_oaid_mapping_v3_view`(
  `oaid_md5` string COMMENT 'oaid_md5',
  `oaid` string COMMENT 'oaid',
  `mac`  array<string> COMMENT 'mac',
  `mac_md5`  array<string> COMMENT 'mac_md5',
  `mac_tm`  array<string> COMMENT 'mac_tm',
  `mac_ltm`  array<string> COMMENT 'mac_ltm',
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
  `orig_imei_ltm` array<string> COMMENT 'orig_imei_ltm'
  )
COMMENT 'oaid=>device映射表'
PARTITIONED BY (
  `day` string COMMENT '创建时间',
  `plat` string COMMENT 'android=1,ios=2')
STORED AS orc;