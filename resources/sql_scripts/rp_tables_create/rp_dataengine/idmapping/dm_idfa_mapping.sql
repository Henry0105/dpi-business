CREATE TABLE dm_dataengine_mapping.dm_idfa_mapping(
  idfa_md5 string,
  idfa string,
  device array<string>,
  device_tm array<string>,
  duid array<string>,
  update_time string COMMENT '最新时间')
  COMMENT 'idfa=>device映射表'
  PARTITIONED BY (
day string COMMENT '创建时间',
  plat string COMMENT 'android=1,ios=2')
stored as orc;