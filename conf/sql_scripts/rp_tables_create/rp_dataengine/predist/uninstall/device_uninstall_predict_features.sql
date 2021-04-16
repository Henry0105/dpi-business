CREATE TABLE IF NOT EXISTS `rp_sdkplus_uninstall.device_uninstall_predict_features`(
  `device` string,
  `appkeys` array<string>,
  `active_7d_idx_sum` array<int> COMMENT '最近7天各app类型活跃天数',
  `device_7d_new_target_app` int COMMENT '最近一周是否新安装目标app（若新安装则值为1 否则值为0）',
  `device_7d_idx_flag_cnt` array<string> COMMENT '最近7天各app类型新安装个数flag=1,最近7天各app类型新卸装个数flag=-1',
  `device_2w_target_app_cnt` int COMMENT '最近两周在目标app上的活跃天数',
  `gender` int COMMENT '',
  `agebin` int COMMENT '',
  `car` int COMMENT '',
  `segment` int COMMENT '人群偏好',
  `occupation` int COMMENT '职业',
  `income` int COMMENT '收入',
  `edu` int COMMENT '教育',
  `kids` int COMMENT '有无未成年子女',
  `house` int COMMENT '房产',
  `married` int COMMENT '婚姻状况',
  `tag_list` map<int, double> COMMENT '在装tag',
  `sysver` int COMMENT '手机操作系统',
  `city_level` int COMMENT '城市等级',
  `factory` int COMMENT '手机品牌',
  `price` double COMMENT '手机价格',
  `month_distance` int COMMENT '手机上市时间距离当前有几个月',
  `applist` array<int> COMMENT '在装各类型app个数',
  `tags_7day` array<int> COMMENT '从rp_sdk_dmp.rp_device_active_label_profile表里取最近7天的设备活跃tag'
)
PARTITIONED BY ( `day` string )
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat';