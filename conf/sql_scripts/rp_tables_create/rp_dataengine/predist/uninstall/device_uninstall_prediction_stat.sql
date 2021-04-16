CREATE TABLE IF NOT EXISTS `rp_sdkplus_uninstall.device_uninstall_prediction_stat`(
  `app_key` string,
  `stat` map<string, int> COMMENT '预测统计,不同分段',
  `process_time` string COMMENT '处理时间',
  `model_day` string COMMENT '模型时间',
  `predict_target_day` string COMMENT '预测数据日期'
)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat';