CREATE TABLE `dm_dataengine_mapping.dm_imsi_mapping_v3`(
  `imsi_md5` string COMMENT 'imsi的md5值',
  `imsi` string COMMENT 'imsi',
  `device` array<string> COMMENT '设备id',
  `device_tm` array<string> COMMENT '设备id的更新时间',
  `duid` array<string> COMMENT 'duid',
  `update_time` string COMMENT '最新时间',
  `device_ltm` array<string> COMMENT '设备id的更新时间',
  `phone` array<string>,
  `phone_md5` array<string>,
  `phone_tm` array<string>,
  `phone_ltm` array<string>,
  `imei` array<string>,
  `imei_md5` array<string>,
  `imei_tm` array<string>,
  `imei_ltm` array<string>,
  `imei_14` array<string>,
  `imei_14_md5` array<string>,
  `imei_14_tm` array<string>,
  `imei_14_ltm` array<string>,
  `imei_15` array<string>,
  `imei_15_md5` array<string>,
  `imei_15_tm` array<string>,
  `imei_15_ltm` array<string>,
  `mac` array<string>,
  `mac_md5` array<string>,
  `mac_tm` array<string>,
  `mac_ltm` array<string>,
  `serialno` array<string>,
  `serialno_tm` array<string>,
  `serialno_ltm` array<string>,
  `orig_imei` array<string> COMMENT 'orig_imei',
  `orig_imei_tm` array<string> COMMENT 'orig_imei_tm',
  `orig_imei_ltm` array<string> COMMENT 'orig_imei_ltm',
  `oaid` array<string> COMMENT 'oaid',
  `oaid_tm` array<string> COMMENT 'oaid_tm',
  `oaid_ltm` array<string> COMMENT 'oaid_ltm'
)
COMMENT 'imsi=>device映射表'
PARTITIONED BY (
  `day` string COMMENT '创建时间',
  `plat` string COMMENT 'android=1,ios=2')
stored as orc;

CREATE TABLE `dm_dataengine_mapping.dm_imsi_mapping_v3_view`(
  `imsi_md5` string COMMENT 'imsi的md5值',
  `imsi` string COMMENT 'imsi',
  `device` array<string> COMMENT '设备id',
  `device_tm` array<string> COMMENT '设备id的更新时间',
  `duid` array<string> COMMENT 'duid',
  `update_time` string COMMENT '最新时间',
  `device_ltm` array<string> COMMENT '设备id的更新时间',
  `phone` array<string>,
  `phone_md5` array<string>,
  `phone_tm` array<string>,
  `phone_ltm` array<string>,
  `imei` array<string>,
  `imei_md5` array<string>,
  `imei_tm` array<string>,
  `imei_ltm` array<string>,
  `imei_14` array<string>,
  `imei_14_md5` array<string>,
  `imei_14_tm` array<string>,
  `imei_14_ltm` array<string>,
  `imei_15` array<string>,
  `imei_15_md5` array<string>,
  `imei_15_tm` array<string>,
  `imei_15_ltm` array<string>,
  `mac` array<string>,
  `mac_md5` array<string>,
  `mac_tm` array<string>,
  `mac_ltm` array<string>,
  `serialno` array<string>,
  `serialno_tm` array<string>,
  `serialno_ltm` array<string>,
  `orig_imei` array<string> COMMENT 'orig_imei',
  `orig_imei_tm` array<string> COMMENT 'orig_imei_tm',
  `orig_imei_ltm` array<string> COMMENT 'orig_imei_ltm',
  `oaid` array<string> COMMENT 'oaid',
  `oaid_tm` array<string> COMMENT 'oaid_tm',
  `oaid_ltm` array<string> COMMENT 'oaid_ltm'
)
COMMENT 'imsi=>device映射表'
PARTITIONED BY (
  `day` string COMMENT '创建时间',
  `plat` string COMMENT 'android=1,ios=2')
stored as orc;