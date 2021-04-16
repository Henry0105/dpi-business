CREATE TABLE `rp_mobdi_app.app_active_weekly`(
  `device` string COMMENT '设备ID',
  `apppkg` string COMMENT 'app名称',
  `days` int COMMENT 'app一周中的活跃天数',
  `day_list` string COMMENT '所有活跃日期的列表，用逗号隔开')
COMMENT '设备的app活跃表(周)'
    PARTITIONED BY (
`par_time` string)
stored as orc