CREATE TABLE `rp_mobdi_app.timewindow_online_profile_day`(
  `device` string,
  `profile` map<string,string>)
COMMENT '生成的标签聚合天表'
PARTITIONED BY (
  `day` string COMMENT '天')
stored as orc;
CREATE TABLE `rp_mobdi_app.profile_history_index`(
  `device` string,
  `feature_index` map<string,struct<file_name:string,row_number:bigint>>)
COMMENT '用来回溯的标签索引表'
PARTITIONED BY (
  `table` string,
  `version` string)
stored as orc;
CREATE TABLE `rp_mobdi_app.timewindow_online_profile_day_v2`(
  `device` string,
  `profile` map<string,string>)
COMMENT '生成的标签聚合天表'
PARTITIONED BY (
  `day` string COMMENT '天')
stored as orc;