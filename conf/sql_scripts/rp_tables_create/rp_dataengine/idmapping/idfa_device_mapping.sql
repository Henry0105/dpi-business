CREATE TABLE IF NOT EXISTS `rp_dataengine.idfa_device_mapping` (
    `idfa_md5` string,
    `idfa` string,
    `device` array<string>,
    `device_tm` array<string>,
    `update_time` string COMMENT '最新时间'
  )
  PARTITIONED BY (
    `day` string COMMENT '创建时间',
    `plat` string COMMENT 'android=1,ios=2'
  ) STORED AS orc;