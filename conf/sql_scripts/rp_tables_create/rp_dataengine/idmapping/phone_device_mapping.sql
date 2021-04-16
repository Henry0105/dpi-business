CREATE TABLE IF NOT EXISTS `rp_dataengine.phone_device_mapping` (
    `phone_md5` string,
    `phone` string,
    `device` array<string>,
    `device_tm` array<string>,
    `update_time` string COMMENT '最新时间'
  )
  PARTITIONED BY (
    `day` string COMMENT '创建时间',
    `plat` string COMMENT 'android=1,ios=2'
  ) STORED AS orc;