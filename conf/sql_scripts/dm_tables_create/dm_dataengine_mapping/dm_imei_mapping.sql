CREATE TABLE `dm_dataengine_mapping.dm_imei_mapping`(
  `imei_md5` string COMMENT 'imei的md5值',
  `imei` string COMMENT 'imei',
  `device` array<string> COMMENT '设备id',
  `device_tm` array<string> COMMENT '设备id的更新时间',
  `duid` array<string> COMMENT 'duid',
  `update_time` string COMMENT 'imei device mapping最新时间',
  `mac` array<string> COMMENT 'mac',
  `mac_tm` array<string> COMMENT 'mac的更新时间',
  `phone` array<string> COMMENT 'phone',
  `phone_tm` array<string> COMMENT 'phone的更新时间')
COMMENT 'imei=>device映射表'
PARTITIONED BY (
  `day` string COMMENT '创建时间',
  `plat` string COMMENT 'android=1,ios=2')
stored as orc;
CREATE TABLE IF NOT EXISTS `dm_dataengine_mapping.dm_imei_mapping_v2` (
  `imei_md5` string,
  `imei` string,
  `device` array<string>,
  `device_tm` array<string>,
  `duid` array<string>,
  `update_time` string COMMENT 'imei device mapping最新时间',
  `mac` array<string>,
  `mac_tm` array<string>,
  `phone` array<string>,
  `phone_tm` array<string>,
  `device_ltm` array<string>,
  `mac_ltm` array<string>,
  `phone_ltm` array<string>
  )
    COMMENT 'imei=>device映射表'
PARTITIONED BY (
  `day` string COMMENT '创建时间',
  `plat` string COMMENT 'android=1,ios=2'
)STORED AS orc;
CREATE TABLE IF NOT EXISTS `dm_dataengine_mapping.dm_imei_mapping_v3` (
  `imei_md5` string,
  `imei` string,
  `imei_14` array<string>,
  `imei_14_md5` array<string>,
  `imei_14_tm` array<string>,
  `imei_14_ltm` array<string>,
  `imei_15` array<string>,
  `imei_15_md5` array<string>,
  `imei_15_tm` array<string>,
  `imei_15_ltm` array<string>,
  `imsi` array<string>,
  `imsi_md5` array<string>,
  `imsi_tm` array<string>,
  `imsi_ltm` array<string>,
  `device` array<string>,
  `device_tm` array<string>,
  `duid` array<string>,
  `update_time` string COMMENT 'imei device mapping最新时间',
  `mac` array<string>,
  `mac_md5` array<string>,
  `mac_tm` array<string>,
  `phone` array<string>,
  `phone_md5` array<string>,
  `phone_tm` array<string>,
  `device_ltm` array<string>,
  `mac_ltm` array<string>,
  `phone_ltm` array<string>,
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
    COMMENT 'imei=>device映射表'
PARTITIONED BY (
  `day` string COMMENT '创建时间',
  `plat` string COMMENT 'android=1,ios=2'
)STORED AS orc;

CREATE TABLE IF NOT EXISTS `dm_dataengine_mapping.dm_imei_mapping_v3_view` (
  `imei_md5` string,
  `imei` string,
  `imei_14` array<string>,
  `imei_14_md5` array<string>,
  `imei_14_tm` array<string>,
  `imei_14_ltm` array<string>,
  `imei_15` array<string>,
  `imei_15_md5` array<string>,
  `imei_15_tm` array<string>,
  `imei_15_ltm` array<string>,
  `imsi` array<string>,
  `imsi_md5` array<string>,
  `imsi_tm` array<string>,
  `imsi_ltm` array<string>,
  `device` array<string>,
  `device_tm` array<string>,
  `duid` array<string>,
  `update_time` string COMMENT 'imei device mapping最新时间',
  `mac` array<string>,
  `mac_md5` array<string>,
  `mac_tm` array<string>,
  `phone` array<string>,
  `phone_md5` array<string>,
  `phone_tm` array<string>,
  `device_ltm` array<string>,
  `mac_ltm` array<string>,
  `phone_ltm` array<string>,
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
    COMMENT 'imei=>device映射表'
PARTITIONED BY (
  `day` string COMMENT '创建时间',
  `plat` string COMMENT 'android=1,ios=2'
)STORED AS orc;