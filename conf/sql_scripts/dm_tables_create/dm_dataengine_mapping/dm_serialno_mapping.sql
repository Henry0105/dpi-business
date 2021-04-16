CREATE TABLE `dm_dataengine_mapping.dm_serialno_mapping_v3`(
  `serialno_md5` string COMMENT 'serialno的md5值',
  `serialno` string COMMENT 'serialno值',
  `device` array<string> COMMENT '设备id',
  `device_tm` array<string> COMMENT '设备id的更新时间',
  `duid` array<string> COMMENT 'duid',
  `update_time` string COMMENT '最新时间',
  `device_ltm` array<string> COMMENT '设备id的更新时间',
  `mac` array<string> COMMENT 'mac',
  `mac_tm` array<string> COMMENT 'mac的更新时间',
  `mac_ltm` array<string> COMMENT 'mac的更新时间',
  `imei` array<string> COMMENT 'imei',
  `imei_tm` array<string> COMMENT 'imei的更新时间',
  `imei_ltm` array<string> COMMENT 'imei的更新时间',
  `imei_14` array<string>,
  `imei_14_tm` array<string>,
  `imei_14_ltm` array<string>,
  `imei_15` array<string>,
  `imei_15_tm` array<string>,
  `imei_15_ltm` array<string>,
  `imsi` array<string>,
  `imsi_tm` array<string>,
  `imsi_ltm` array<string>,
  `phone` array<string>,
  `phone_tm` array<string>,
  `phone_ltm` array<string>,
  `orig_imei` array<string> COMMENT 'orig_imei',
  `orig_imei_tm` array<string> COMMENT 'orig_imei_tm',
  `orig_imei_ltm` array<string> COMMENT 'orig_imei_ltm',
  `oaid` array<string> COMMENT 'oaid',
  `oaid_tm` array<string> COMMENT 'oaid_tm',
  `oaid_ltm` array<string> COMMENT 'oaid_ltm'
)
COMMENT 'serialno=>device映射表'
PARTITIONED BY (
  `day` string COMMENT '创建时间',
  `plat` string COMMENT 'android=1,ios=2')
stored as orc;

CREATE TABLE `dm_dataengine_mapping.dm_serialno_mapping_v3_view`(
  `serialno_md5` string COMMENT 'serialno的md5值',
  `serialno` string COMMENT 'serialno值',
  `device` array<string> COMMENT '设备id',
  `device_tm` array<string> COMMENT '设备id的更新时间',
  `duid` array<string> COMMENT 'duid',
  `update_time` string COMMENT '最新时间',
  `device_ltm` array<string> COMMENT '设备id的更新时间',
  `mac` array<string> COMMENT 'mac',
  `mac_tm` array<string> COMMENT 'mac的更新时间',
  `mac_ltm` array<string> COMMENT 'mac的更新时间',
  `imei` array<string> COMMENT 'imei',
  `imei_tm` array<string> COMMENT 'imei的更新时间',
  `imei_ltm` array<string> COMMENT 'imei的更新时间',
  `imei_14` array<string>,
  `imei_14_tm` array<string>,
  `imei_14_ltm` array<string>,
  `imei_15` array<string>,
  `imei_15_tm` array<string>,
  `imei_15_ltm` array<string>,
  `imsi` array<string>,
  `imsi_tm` array<string>,
  `imsi_ltm` array<string>,
  `phone` array<string>,
  `phone_tm` array<string>,
  `phone_ltm` array<string>,
  `orig_imei` array<string> COMMENT 'orig_imei',
  `orig_imei_tm` array<string> COMMENT 'orig_imei_tm',
  `orig_imei_ltm` array<string> COMMENT 'orig_imei_ltm',
  `oaid` array<string> COMMENT 'oaid',
  `oaid_tm` array<string> COMMENT 'oaid_tm',
  `oaid_ltm` array<string> COMMENT 'oaid_ltm'
)
COMMENT 'serialno=>device映射表'
PARTITIONED BY (
  `day` string COMMENT '创建时间',
  `plat` string COMMENT 'android=1,ios=2')
stored as orc;