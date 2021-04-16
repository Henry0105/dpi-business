CREATE TABLE IF NOT EXISTS `dm_mobdi_master.device_staying_daily`(
  `device` string COMMENT 'device_id',
  `duid` string COMMENT 'duid',
  `lat` string COMMENT 'bd09坐标系',
  `lon` string COMMENT 'bd09坐标系',
  `start_time` string COMMENT '设备在该经纬度上传记录的时间',
  `end_time` string COMMENT '设备在按时间排序的下一个非该经纬度上传记录的时间',
  `processtime` string COMMENT '用于标示该记录属于哪一天',
  `country` string COMMENT '国家编码',
  `province` string COMMENT '省份编码',
  `city` string COMMENT '城市编码',
  `area` string COMMENT '地区编码',
  `street` string COMMENT '街道名称',
  `plat` string COMMENT '所属系统平台',
  `network` string COMMENT '网络状态',
  `type` string COMMENT '获取经纬度的方式，分区字段，类型有GPS，IP，wifi，base，tz，wifilist',
  `data_source` string COMMENT '来源表',
  `orig_note1` string COMMENT '各种方式获取经纬度的信息1',
  `orig_note2` string COMMENT '各种方式获取经纬度的信息2',
  `accuracy` string COMMENT '准确度')
COMMENT '经纬度master表,在经纬度原始表的基础上进行聚合，在时间上进行排序，如果相邻记录a，b经纬度相同，下一条记录c跟a、b的经纬度不同，则删除后一条记录b，a记录生成一条新记录a\'，a中的time作为a\'中的time作为start_time，c中的time作为新的记录a\'中的end_time'
PARTITIONED BY (
  `day` string COMMENT '日期')
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  '/tmp/hive';