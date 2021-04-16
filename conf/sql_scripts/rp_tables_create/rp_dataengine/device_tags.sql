CREATE TABLE IF NOT EXISTS `device_tags` (
  `device` string,
  `finance_time` string,
  `finance_action` string,
  `inital_ts` string,
  `portrait_cook_tag` string,
  `installed_cate_tag` string,
  `portrait_tour_tag` string,
  `installed_app_tag` string,
  `imei` string,
  `cell_factory` int,
  `gender` int,
  `agebin` int,
  `segment` int,
  `edu` int,
  `kids` int,
  `income` int,
  `model_level` int,
  `tot_install_apps` int,
  `tags` string,
  `model` string,
  `carrier` string,
  `network` string,
  `screensize` string,
  `sysver` string,
  `city_level` int,
  `permanent_country` string,
  `permanent_province` string,
  `permanent_city` string,
  `occupation` int,
  `house` int,
  `repayment` int,
  `car` int,
  `workplace` string,
  `residence` string,
  `married` int,
  `update_time` string
)
COMMENT '设备全量标签'
PARTITIONED BY (
  `day` string COMMENT '全量数据,A|B分区切换'
) STORED AS orc;
