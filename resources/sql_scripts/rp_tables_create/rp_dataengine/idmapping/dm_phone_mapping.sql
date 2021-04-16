CREATE TABLE dm_dataengine_mapping.dm_phone_mapping(
  phone_md5 string,
  phone string,
  device_plat array<string> COMMENT 'android=1,ios=2',
  device array<string>,
  device_tm array<string>,
  duid array<string>,
  update_time string COMMENT '最新时间')
  COMMENT 'phone=>device映射表'
  PARTITIONED BY (
day string COMMENT '创建时间')
stored as orc;