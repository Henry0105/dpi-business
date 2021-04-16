CREATE TABLE IF NOT EXISTS `DEVICE_OPT_CACHE` (
    `device` string
  )
  COMMENT '设备操作缓存表,需要定期清理'
  PARTITIONED BY (
    `created_day` string COMMENT '创建时间',
    `uuid` string COMMENT '唯一标识外界指定'
  ) STORED AS orc;