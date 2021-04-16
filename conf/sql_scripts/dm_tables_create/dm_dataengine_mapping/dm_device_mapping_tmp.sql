CREATE TABLE `dm_dataengine_mapping.dm_device_mapping_tmp`(
  `device` string COMMENT '设备id',
  `imei` array<string> COMMENT 'imei',
  `imei_14` array<string> COMMENT '14位imei',
  `imei_tm` array<string> COMMENT 'imei更新时间',
  `idfa` array<string> COMMENT 'idfa',
  `idfa_tm` array<string> COMMENT 'idfa更新时间',
  `mac` array<string> COMMENT 'mac',
  `mac_tm` array<string> COMMENT 'mac更新时间',
  `phone` array<string> COMMENT 'phone',
  `phone_tm` array<string> COMMENT 'phone更新时间',
  `imsi` array<string> COMMENT 'imsi',
  `imsi_tm` array<string> COMMENT 'imsi更新时间',
  `duid` array<string> COMMENT 'duid',
  `serialno` array<string> COMMENT 'serialno',
  `serialno_tm` array<string> COMMENT 'serialno更新时间',
  `update_time` string COMMENT 'imei,idfa,mac,phone,imsi,serialno mapping最新时间',
  `imei_ltm` array<string> COMMENT 'imei最后活跃时间',
  `idfa_ltm` array<string> COMMENT 'idfa最后活跃时间',
  `mac_ltm` array<string> COMMENT 'mac最后活跃时间',
  `phone_ltm` array<string> COMMENT 'phone最后活跃时间',
  `imsi_ltm` array<string> COMMENT 'imsi最后活跃时间',
  `serialno_ltm` array<string> COMMENT 'serialno最后活跃时间',
  `day` string COMMENT '创建时间',
  `plat` string COMMENT 'android=1,ios=2')
COMMENT 'device=>imei mac phone idfa imsi imei_14映射'
row format delimited
fields terminated by '\t'
collection items terminated by ','
lines terminated by '\n'
tblproperties("skip.header.line.count"="1");
