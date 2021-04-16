CREATE TABLE dm_dataengine_mapping.dm_imei_mapping(
  imei_md5 string,
  imei string,
  device array<string>,
  device_tm array<string>,
  duid array<string>,
  update_time string COMMENT 'imei device mapping最新时间')
  COMMENT 'imei=>device映射表'
  PARTITIONED BY (
day string COMMENT '创建时间',
  plat string COMMENT 'android=1,ios=2')
stored as orc;