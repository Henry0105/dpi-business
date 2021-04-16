CREATE TABLE IF NOT EXISTS `rp_mobeye_app360.rp_app_rank_category_insatll_monthly`(
  `apppkg` string COMMENT '清理后包名',
  `zone` string COMMENT '地区',
  `cate_id` string COMMENT '分类id',
  `gender` int COMMENT '年龄',
  `agebin` int COMMENT '性别',
  `model_level` int COMMENT '设备价位',
  `install_cnt` bigint COMMENT '在装数量',
  `install_incr_cnt` bigint COMMENT '新安装数量',
  `unstall_cnt` bigint COMMENT '卸载数量',
  `install_percent` double COMMENT '安装渗透率',
  `device_cnt` bigint COMMENT '总的设备数量',
  `rank_install` bigint COMMENT '在装排名',
  `rank_install_incr` bigint COMMENT '新安装排名',
  `rank_unstall` bigint COMMENT '卸载排名',
  `install_cnt_esti` bigint COMMENT '在装数量推算',
  `install_incr_cnt_esti` bigint COMMENT '新安装数量推算',
  `unstall_cnt_esti` bigint COMMENT '卸载数量推算',
  `install_percent_esti` double COMMENT '安装渗透率推算',
  `device_cnt_esti` bigint COMMENT '总的设备数量推算',
  `rank_install_esti` bigint COMMENT '在装排名推算',
  `rank_install_incr_esti` bigint COMMENT '新安装排名推算',
  `rank_unstall_esti` bigint COMMENT '卸载排名推算')
COMMENT '月安装排行榜'
PARTITIONED BY (
  `rank_date` string COMMENT '日期')
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat';
