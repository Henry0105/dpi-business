CREATE TABLE dm_dataengine_mapping.dm_imei14_mapping(
  imei_14_md5 string,
  imei_14 string,
  device array<string>,
  device_tm array<string>,
  duid array<string>,
  update_time string COMMENT 'imei device mapping最新时间')
  COMMENT 'imei_14=>device映射表'
  PARTITIONED BY (
day string COMMENT '创建时间',
  plat string COMMENT 'android=1,ios=2')
STORED as orc;