CREATE TABLE `dm_dataengine_mapping.dm_pid_mapping_v3_bk`(
  `pid` string COMMENT 'pid',
  `bk_tm` array<string> COMMENT '回溯时间戳,记录了pid<=>device的时间戳,跟device_index数组拉链使用',
  `device_index` array<int> COMMENT '记录device_plat,device,device_tm的数组下标',
  `device` array<string> COMMENT '设备id',
  `update_time` string COMMENT '更新时间'
)
COMMENT 'pid=>device映射表'
PARTITIONED BY (
  `day` string COMMENT '创建时间')
stored as orc;