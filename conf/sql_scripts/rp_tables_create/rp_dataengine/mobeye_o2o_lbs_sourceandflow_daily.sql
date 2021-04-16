CREATE TABLE `rp_dataengine.mobeye_o2o_lbs_soureandflow_daily`(
  `storeid` string COMMENT '店铺',
  `device` string COMMENT '设备号',
  `gender` int COMMENT '性别',
  `agebin` int COMMENT '年龄',
  `segment` int COMMENT '人群',
  `edu` int COMMENT '教育程度',
  `kids` int COMMENT '有无小孩',
  `income` int COMMENT '收入',
  `occupation` string COMMENT '职业',
  `house` int COMMENT '房产情况',
  `repayment` int COMMENT '偿还能力',
  `car` int COMMENT '车产情况',
  `married` int COMMENT '婚姻状况',
  `source` struct<lat:string,lon:string,province:string,city:string,area:string> COMMENT '来源',
  `s_geohash_center5` struct<lat:string,lon:string> COMMENT '5位geohash',
  `s_geohash_center6` struct<lat:string,lon:string> COMMENT '6位geohash',
  `s_geohash_center7` struct<lat:string,lon:string> COMMENT '7位geohash',
  `s_geohash_center8` struct<lat:string,lon:string> COMMENT '8位geohash',
  `s_type_1` struct<lat:string,lon:string,name:string> COMMENT '类型1：小区',
  `s_type_2` struct<lat:string,lon:string,name:string> COMMENT '类型2：办公',
  `s_type_3` struct<lat:string,lon:string,name:string> COMMENT '类型3：商圈',
  `flow` struct<lat:string,lon:string,province:string,city:string,area:string> COMMENT '去向',
  `f_geohash_center5` struct<lat:string,lon:string> COMMENT '5位geohash',
  `f_geohash_center6` struct<lat:string,lon:string> COMMENT '6位geohash',
  `f_geohash_center7` struct<lat:string,lon:string> COMMENT '7位geohash',
  `f_geohash_center8` struct<lat:string,lon:string> COMMENT '8位geohash',
  `f_type_1` struct<lat:string,lon:string,name:string> COMMENT '类型1：小区',
  `f_type_2` struct<lat:string,lon:string,name:string> COMMENT '类型2：办公',
  `f_type_3` struct<lat:string,lon:string,name:string> COMMENT '类型3：商圈',
  `hour` string COMMENT '小时')
COMMENT '全国客流来源去向表'
PARTITIONED BY (
  `rank_date` string COMMENT '日期',
  `userid` string COMMENT '用户号')
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  '/tmp/hive'