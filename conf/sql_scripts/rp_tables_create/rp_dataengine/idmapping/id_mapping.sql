CREATE TABLE IF NOT EXISTS `rp_dataengine.id_mapping` (
    `device` string,
    `imei` array<string>,
    `imei_14` array<string>,
    `imei_tm` array<string>,
    `idfa` array<string>,
    `idfa_tm` array<string>,
    `mac` array<string>,
    `mac_tm` array<string>,
    `phone` array<string>,
    `phone_tm` array<string>,
    `imsi` array<string>,
    `imsi_tm` array<string>,
    `update_time` string COMMENT 'imei,idfa,mac,phone,imsi mapping最新时间'
  )
  PARTITIONED BY (
    `day` string COMMENT '创建时间',
    `plat` string COMMENT 'android=1,ios=2'
  ) STORED AS orc;