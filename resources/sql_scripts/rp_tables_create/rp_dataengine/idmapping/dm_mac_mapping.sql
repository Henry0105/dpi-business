CREATE TABLE dm_dataengine_mapping.dm_mac_mapping(
  mac_md5 string,
  mac string,
  device_plat array<string> COMMENT 'android=1,ios=2',
  device array<string>,
  device_tm array<string>,
  duid array<string>,
  update_time string COMMENT '最新时间')
  COMMENT 'mac=>device映射表'
  PARTITIONED BY (
day string COMMENT '创建时间')
STORED as orc;