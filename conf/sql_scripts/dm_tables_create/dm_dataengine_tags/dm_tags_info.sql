CREATE TABLE `dm_dataengine_tags.dm_tags_info`(
  `device` string COMMENT '设备',
  `tags` map<string,string> COMMENT '标签id和值的map',
  `confidence` map<string,string> COMMENT '标签id和置信度的map',
  `update_time` string COMMENT '这条记录最新更新时间',
  `grouped_tags` map<string,string> COMMENT '根据标签id的父分类id聚合的map')
COMMENT '根据标签系统生成的设备标签'
PARTITIONED BY (
  `day` string COMMENT '日期')
  stored as orc;

CREATE TABLE `dm_dataengine_tags.dm_tags_info_view`(
  `device` string COMMENT '设备',
  `tags` map<string,string> COMMENT '标签id和值的map',
  `confidence` map<string,string> COMMENT '标签id和置信度的map',
  `update_time` string COMMENT '这条记录最新更新时间',
  `grouped_tags` map<string,string> COMMENT '根据标签id的父分类id聚合的map')
COMMENT '根据标签系统生成的设备标签'
PARTITIONED BY (
  `day` string COMMENT '日期')
  stored as orc;