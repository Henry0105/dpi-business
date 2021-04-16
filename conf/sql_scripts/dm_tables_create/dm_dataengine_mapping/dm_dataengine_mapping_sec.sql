CREATE TABLE `dm_dataengine_test.dm_device_mapping_sec_view`(
  `device` string COMMENT '设备id',
  `ieid` array<string> COMMENT 'ieid',
  `ieid_tm` array<string> COMMENT 'ieid更新时间',
  `ifid` array<string> COMMENT 'ifid',
  `ifid_tm` array<string> COMMENT 'ifid更新时间',
  `mcid` array<string> COMMENT 'mcid',
  `mcid_tm` array<string> COMMENT 'mcid更新时间',
  `pid` array<string> COMMENT 'pid',
  `pid_tm` array<string> COMMENT 'pid更新时间',
  `isid` array<string> COMMENT 'isid',
  `isid_tm` array<string> COMMENT 'isid更新时间',
  `duid` array<string> COMMENT 'duid',
  `snid` array<string> COMMENT 'snid',
  `snid_tm` array<string> COMMENT 'snid更新时间',
  `update_time` string COMMENT 'ieid,ifid,mcid,pid,isid,snid mapping最新时间',
  `ieid_ltm` array<string> COMMENT 'ieid最后活跃时间',
  `ifid_ltm` array<string> COMMENT 'ifid最后活跃时间',
  `mcid_ltm` array<string> COMMENT 'mcid最后活跃时间',
  `pid_ltm` array<string> COMMENT 'pid最后活跃时间',
  `isid_ltm` array<string> COMMENT 'isid最后活跃时间',
  `snid_ltm` array<string> COMMENT 'snid最后活跃时间',
  `orig_ieid` array<string> COMMENT 'orig_ieid',
  `orig_ieid_tm` array<string> COMMENT 'orig_ieid_tm',
  `orig_ieid_ltm` array<string> COMMENT 'orig_ieid_ltm',
  `oiid` array<string> COMMENT 'oiid',
  `oiid_tm` array<string> COMMENT 'oiid_tm',
  `oiid_ltm` array<string> COMMENT 'oiid_ltm'
  )
COMMENT 'device=>ieid mcid pid ifid isid ieid_14映射'
PARTITIONED BY (
  `day` string COMMENT '创建时间',
  `plat` string COMMENT 'android=1,ios=2')
  stored as orc;

CREATE TABLE `dm_dataengine_test.dm_ifid_mapping_view`(
  `ifid` string COMMENT 'ifid',
  `device` array<string> COMMENT '设备id',
  `device_tm` array<string> COMMENT '设备id的更新时间',
  `duid` array<string> COMMENT 'duid',
  `update_time` string COMMENT '最新时间',
  `device_ltm` array<string> COMMENT '设备id的更新时间',
  `pid` array<string>,
  `pid_tm` array<string>,
  `pid_ltm` array<string>,
  `mcid` array<string>,
  `mcid_tm` array<string>,
  `mcid_ltm` array<string>
)
COMMENT 'ifid=>device映射表'
PARTITIONED BY (
  `day` string COMMENT '创建时间',
  `plat` string COMMENT 'android=1,ios=2')
stored as orc;

CREATE TABLE IF NOT EXISTS `dm_dataengine_test.dm_ieid_mapping_view` (
  `ieid` string,
  `isid` array<string>,
  `isid_tm` array<string>,
  `isid_ltm` array<string>,
  `device` array<string>,
  `device_tm` array<string>,
  `duid` array<string>,
  `update_time` string COMMENT 'ieid device mapping最新时间',
  `mcid` array<string>,
  `mcid_tm` array<string>,
  `pid` array<string>,
  `pid_tm` array<string>,
  `device_ltm` array<string>,
  `mcid_ltm` array<string>,
  `pid_ltm` array<string>,
  `snid` array<string>,
  `snid_tm` array<string>,
  `snid_ltm` array<string>,
  `orig_ieid` array<string> COMMENT 'orig_ieid',
  `orig_ieid_tm` array<string> COMMENT 'orig_ieid_tm',
  `orig_ieid_ltm` array<string> COMMENT 'orig_ieid_ltm',
  `oiid` array<string> COMMENT 'oiid',
  `oiid_tm` array<string> COMMENT 'oiid_tm',
  `oiid_ltm` array<string> COMMENT 'oiid_ltm'
  )
    COMMENT 'ieid=>device映射表'
PARTITIONED BY (
  `day` string COMMENT '创建时间',
  `plat` string COMMENT 'android=1,ios=2'
)STORED AS orc;

CREATE TABLE `dm_dataengine_test.dm_isid_mapping_view`(
  `isid` string COMMENT 'isid',
  `device` array<string> COMMENT '设备id',
  `device_tm` array<string> COMMENT '设备id的更新时间',
  `duid` array<string> COMMENT 'duid',
  `update_time` string COMMENT '最新时间',
  `device_ltm` array<string> COMMENT '设备id的更新时间',
  `pid` array<string>,
  `pid_tm` array<string>,
  `pid_ltm` array<string>,
  `ieid` array<string>,
  `ieid_tm` array<string>,
  `ieid_ltm` array<string>,
  `mcid` array<string>,
  `mcid_tm` array<string>,
  `mcid_ltm` array<string>,
  `snid` array<string>,
  `snid_tm` array<string>,
  `snid_ltm` array<string>,
  `orig_ieid` array<string> COMMENT 'orig_ieid',
  `orig_ieid_tm` array<string> COMMENT 'orig_ieid_tm',
  `orig_ieid_ltm` array<string> COMMENT 'orig_ieid_ltm',
  `oiid` array<string> COMMENT 'oiid',
  `oiid_tm` array<string> COMMENT 'oiid_tm',
  `oiid_ltm` array<string> COMMENT 'oiid_ltm'
)
COMMENT 'isid=>device映射表'
PARTITIONED BY (
  `day` string COMMENT '创建时间',
  `plat` string COMMENT 'android=1,ios=2')
stored as orc;
CREATE TABLE dm_dataengine_test.dm_mcid_mapping_view(
  mcid string COMMENT 'mcid',
  device_plat array<string> COMMENT '设备id的平台(android=1,ios=2)',
  device array<string> COMMENT '设备id',
  device_tm array<string> COMMENT '设备id的更新时间',
  duid array<string> COMMENT 'duid',
  update_time string COMMENT '最新时间',
  ieid array<string> COMMENT 'ieid',
  ieid_tm array<string> COMMENT 'ieid更新时间',
  pid array<string> COMMENT 'pid',
  pid_tm array<string> COMMENT 'pid更新时间',
  device_ltm array<string> COMMENT '设备id的最后活跃时间',
  ieid_ltm array<string> COMMENT 'ieid的最后活跃时间',
  pid_ltm array<string> COMMENT 'pid更新时间',
  isid array<string>,
  isid_tm array<string>,
  isid_ltm array<string>,
  snid array<string>,
  snid_tm array<string>,
  snid_ltm array<string>,
  ifid array<string>,
  ifid_tm array<string>,
  ifid_ltm array<string>,
  orig_ieid array<string>,
  orig_ieid_tm array<string>,
  orig_ieid_ltm array<string>,
  oiid array<string>,
  oiid_tm array<string>,
  oiid_ltm array<string>)
COMMENT 'mcid=>device映射表'
PARTITIONED BY (
  day string COMMENT '创建时间')
stored as orc;
CREATE TABLE dm_dataengine_test.dm_pid_mapping_view(
  pid string COMMENT 'pid',
  device_plat array<string> COMMENT 'android=1,ios=2',
  device array<string> COMMENT '设备id',
  device_tm array<string> COMMENT '设备id的更新时间',
  duid array<string> COMMENT 'duid',
  update_time string COMMENT '最新时间',
  mcid array<string> COMMENT 'mcid',
  mcid_tm array<string> COMMENT 'mcid的更新时间',
  ieid array<string> COMMENT 'ieid',
  ieid_tm array<string> COMMENT 'ieid的更新时间',
  device_ltm array<string> COMMENT '设备id的更新时间',
  mcid_ltm array<string> COMMENT 'mcid的更新时间',
  ieid_ltm array<string> COMMENT 'ieid的更新时间',
  isid array<string>,
  isid_tm array<string>,
  isid_ltm array<string>,
  snid array<string>,
  snid_tm array<string>,
  snid_ltm array<string>,
  ifid array<string>,
  ifid_tm array<string>,
  ifid_ltm array<string>,
  orig_ieid array<string>,
  orig_ieid_tm array<string>,
  orig_ieid_ltm array<string>,
  oiid array<string>,
  oiid_tm array<string>,
  oiid_ltm array<string>)
COMMENT 'phone=>device映射表'
PARTITIONED BY (
  day string COMMENT '创建时间')
stored as orc;

CREATE TABLE dm_dataengine_test.id_mapping_sec_external_full(
  owner_data string COMMENT '交换出去的数据',
  ext_data array<string> COMMENT '反馈的数据 列表',
  ext_data_tm array<string> COMMENT '处理时间 列表')
COMMENT '交换数据mapping 增量表'
PARTITIONED BY (
  day string COMMENT '时间分区',
  type string COMMENT '数据类别')
stored as orc;

CREATE TABLE dm_dataengine_test.id_mapping_sec_external_full_inc(
  owner_data string COMMENT '交换出去的数据',
  ext_data array<string> COMMENT '反馈的数据 列表',
  ext_data_tm array<string> COMMENT '处理时间 列表')
COMMENT '交换数据mapping 增量表'
PARTITIONED BY (
  day string COMMENT '时间分区',
  type string COMMENT '数据类别')
stored as orc;


CREATE TABLE dm_dataengine_test.id_mapping_sec_external_full_inc_view(
  owner_data string COMMENT '交换出去的数据',
  ext_data array<string> COMMENT '反馈的数据 列表',
  ext_data_tm array<string> COMMENT '处理时间 列表')
stored as orc;

CREATE TABLE dm_dataengine_test.id_mapping_sec_external_full_view(
  owner_data string COMMENT '交换出去的数据',
  ext_data array<string> COMMENT '反馈的数据 列表',
  ext_data_tm array<string> COMMENT '处理时间 列表')
stored as orc;

CREATE TABLE mobdi_test.zjc_ext_pid_mapping_incr (
 owner_data  string COMMENT '交换出去的数据',
 ext_data  string COMMENT '反馈的数据',
 source  int COMMENT '数据源',
 plat  int COMMENT '1 安卓 2 ios',
 processtime  string COMMENT '处理时间')
COMMENT '交换数据mapping'
PARTITIONED BY (
 day  string COMMENT '时间分区',
 type  string COMMENT '数据类别')
stored as orc;

CREATE TABLE dm_dataengine_test.dm_snid_mapping_view(
snid string COMMENT 'snid值',
device array<string> COMMENT '设备id',
device_tm array<string> COMMENT '设备id的更新时间',
duid array<string> COMMENT 'duid',
update_time string COMMENT '最新时间',
device_ltm array<string> COMMENT '设备id的更新时间',
mcid array<string> COMMENT 'mcid',
mcid_tm array<string> COMMENT 'mcid的更新时间',
mcid_ltm array<string> COMMENT 'mcid的更新时间',
ieid array<string> COMMENT 'ieid',
ieid_tm array<string> COMMENT 'ieid的更新时间',
ieid_ltm array<string> COMMENT 'ieid的更新时间',
isid array<string>,
isid_tm array<string>,
isid_ltm array<string>,
pid array<string>,
pid_tm array<string>,
pid_ltm array<string>,
orig_ieid array<string>,
orig_ieid_tm array<string>,
orig_ieid_ltm array<string>,
oiid array<string>,
oiid_tm array<string>,
oiid_ltm array<string>)
COMMENT 'snid=>device映射表'
PARTITIONED BY (
day string COMMENT '创建时间',
plat string COMMENT 'android=1,ios=2')
stored as orc;
CREATE TABLE dm_dataengine_test.dm_oiid_mapping_view(
oiid string COMMENT 'oiid',
mcid array<string> COMMENT 'mcid',
mcid_tm array<string> COMMENT 'mcid_tm',
mcid_ltm array<string> COMMENT 'mcid_ltm',
device array<string> COMMENT '设备id',
device_tm array<string> COMMENT '设备id的更新时间',
duid array<string> COMMENT 'duid',
update_time string COMMENT '最新时间',
ieid array<string> COMMENT 'ieid',
ieid_tm array<string> COMMENT 'ieid更新时间',
pid array<string> COMMENT 'pid',
pid_tm array<string> COMMENT 'pid更新时间',
device_ltm array<string> COMMENT '设备id的最后活跃时间',
ieid_ltm array<string> COMMENT 'ieid的最后活跃时间',
pid_ltm array<string> COMMENT 'pid更新时间',
isid array<string>,
isid_tm array<string>,
isid_ltm array<string>,
snid array<string>,
snid_tm array<string>,
snid_ltm array<string>,
ifid array<string>,
ifid_tm array<string>,
ifid_ltm array<string>,
orig_ieid array<string> COMMENT 'orig_ieid',
orig_ieid_tm array<string> COMMENT 'orig_ieid_tm',
orig_ieid_ltm array<string> COMMENT 'orig_ieid_ltm')
COMMENT 'oiid=>device映射表'
PARTITIONED BY (
day string COMMENT '创建时间',
plat string COMMENT 'android=1,ios=2')
stored as orc;