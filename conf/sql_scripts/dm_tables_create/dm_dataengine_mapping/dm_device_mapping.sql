CREATE TABLE `dm_dataengine_mapping.dm_device_mapping`(
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
  `update_time` string COMMENT 'imei,idfa,mac,phone,imsi,serialno mapping最新时间')
COMMENT 'device=>imei mac phone idfa imsi imei_14映射'
PARTITIONED BY (
  `day` string COMMENT '创建时间',
  `plat` string COMMENT 'android=1,ios=2')
stored as orc;
CREATE TABLE `dm_dataengine_mapping.dm_device_mapping_v2`(
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
  `serialno_ltm` array<string> COMMENT 'serialno最后活跃时间')
COMMENT 'device=>imei mac phone idfa imsi imei_14映射'
PARTITIONED BY (
  `day` string COMMENT '创建时间',
  `plat` string COMMENT 'android=1,ios=2')
  stored as orc;
CREATE TABLE `dm_dataengine_mapping.dm_device_mapping_v3`(
  `device` string COMMENT '设备id',
  `imei` array<string> COMMENT 'imei',
  `imei_14` array<string> COMMENT '14位imei',
  `imei_15` array<string> COMMENT '15位imei',
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
  `imei_14_tm` array<string> COMMENT 'imei14更新时间',
  `imei_15_tm` array<string> COMMENT 'imei15更新时间',
  `imei_14_ltm` array<string> COMMENT 'imei14最后活跃时间',
  `imei_15_ltm` array<string> COMMENT 'imei15最后活跃时间',
  `idfa_ltm` array<string> COMMENT 'idfa最后活跃时间',
  `mac_ltm` array<string> COMMENT 'mac最后活跃时间',
  `phone_ltm` array<string> COMMENT 'phone最后活跃时间',
  `imsi_ltm` array<string> COMMENT 'imsi最后活跃时间',
  `serialno_ltm` array<string> COMMENT 'serialno最后活跃时间',
  `orig_imei` array<string> COMMENT 'orig_imei',
  `orig_imei_tm` array<string> COMMENT 'orig_imei_tm',
  `orig_imei_ltm` array<string> COMMENT 'orig_imei_ltm',
  `oaid` array<string> COMMENT 'oaid',
  `oaid_tm` array<string> COMMENT 'oaid_tm',
  `oaid_ltm` array<string> COMMENT 'oaid_ltm'
  )
COMMENT 'device=>imei mac phone idfa imsi imei_14映射'
PARTITIONED BY (
  `day` string COMMENT '创建时间',
  `plat` string COMMENT 'android=1,ios=2')
  stored as orc;
CREATE TABLE IF NOT EXISTS `dm_dataengine_mapping.id_mapping_external_full`(
  `owner_data` string COMMENT '交换出去的数据',
  `owner_data_md5` string COMMENT '交换出去的数据 md5加密',
  `ext_data` array<string> COMMENT '反馈的数据 列表',
  `ext_data_md5` array<string> COMMENT '反馈的数据 md5加密 列表',
  `ext_data_tm` array<string> COMMENT '处理时间 列表')
COMMENT '交换数据mapping 全量表'
PARTITIONED BY (
  `day` string COMMENT '时间分区',
  `type` string COMMENT '数据类别')
  stored as orc;
CREATE TABLE `dm_dataengine_mapping.dm_device_mapping_v3_inc`(
  `device` string COMMENT '设备id',
  `imei` array<string> COMMENT 'imei',
  `imei_14` array<string> COMMENT '14位imei',
  `imei_15` array<string> COMMENT '15位imei',
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
  `imei_14_tm` array<string> COMMENT 'imei14更新时间',
  `imei_15_tm` array<string> COMMENT 'imei15更新时间',
  `imei_14_ltm` array<string> COMMENT 'imei14最后活跃时间',
  `imei_15_ltm` array<string> COMMENT 'imei15最后活跃时间',
  `idfa_ltm` array<string> COMMENT 'idfa最后活跃时间',
  `mac_ltm` array<string> COMMENT 'mac最后活跃时间',
  `phone_ltm` array<string> COMMENT 'phone最后活跃时间',
  `imsi_ltm` array<string> COMMENT 'imsi最后活跃时间',
  `serialno_ltm` array<string> COMMENT 'serialno最后活跃时间',
  `orig_imei` array<string> COMMENT 'orig_imei',
  `orig_imei_tm` array<string> COMMENT 'orig_imei_tm',
  `orig_imei_ltm` array<string> COMMENT 'orig_imei_ltm',
  `oaid` array<string> COMMENT 'oaid',
  `oaid_tm` array<string> COMMENT 'oaid_tm',
  `oaid_ltm` array<string> COMMENT 'oaid_ltm'
  )
COMMENT 'device=>imei mac phone idfa imsi imei_14映射 增量表'
PARTITIONED BY (
  `day` string COMMENT '创建时间',
  `plat` string COMMENT 'android=1,ios=2')
  stored as orc;

  CREATE TABLE IF NOT EXISTS `dm_dataengine_mapping.id_mapping_external_full_inc`(
  `owner_data` string COMMENT '交换出去的数据',
  `owner_data_md5` string COMMENT '交换出去的数据 md5加密',
  `ext_data` array<string> COMMENT '反馈的数据 列表',
  `ext_data_md5` array<string> COMMENT '反馈的数据 md5加密 列表',
  `ext_data_tm` array<string> COMMENT '处理时间 列表')
COMMENT '交换数据mapping 增量表'
PARTITIONED BY (
  `day` string COMMENT '时间分区',
  `type` string COMMENT '数据类别')
  stored as orc;

  CREATE TABLE `dm_dataengine_mapping.dm_device_mapping_v3_view`(
  `device` string COMMENT '设备id',
  `imei` array<string> COMMENT 'imei',
  `imei_14` array<string> COMMENT '14位imei',
  `imei_15` array<string> COMMENT '15位imei',
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
  `imei_14_tm` array<string> COMMENT 'imei14更新时间',
  `imei_15_tm` array<string> COMMENT 'imei15更新时间',
  `imei_14_ltm` array<string> COMMENT 'imei14最后活跃时间',
  `imei_15_ltm` array<string> COMMENT 'imei15最后活跃时间',
  `idfa_ltm` array<string> COMMENT 'idfa最后活跃时间',
  `mac_ltm` array<string> COMMENT 'mac最后活跃时间',
  `phone_ltm` array<string> COMMENT 'phone最后活跃时间',
  `imsi_ltm` array<string> COMMENT 'imsi最后活跃时间',
  `serialno_ltm` array<string> COMMENT 'serialno最后活跃时间',
  `orig_imei` array<string> COMMENT 'orig_imei',
  `orig_imei_tm` array<string> COMMENT 'orig_imei_tm',
  `orig_imei_ltm` array<string> COMMENT 'orig_imei_ltm',
  `oaid` array<string> COMMENT 'oaid',
  `oaid_tm` array<string> COMMENT 'oaid_tm',
  `oaid_ltm` array<string> COMMENT 'oaid_ltm'
  )
COMMENT 'device=>imei mac phone idfa imsi imei_14映射'
PARTITIONED BY (
  `day` string COMMENT '创建时间',
  `plat` string COMMENT 'android=1,ios=2')
  stored as orc;
