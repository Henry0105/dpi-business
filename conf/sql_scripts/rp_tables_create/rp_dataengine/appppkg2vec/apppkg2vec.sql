CREATE TABLE IF NOT EXISTS `rp_dataengine.apppkg_vector_data_opt`(
  `app_name` string COMMENT '应用名称',
  `features` array<double> COMMENT '稠密向量特征')
COMMENT 'appname特征表'
PARTITIONED BY (
  `day` string COMMENT '入库日期',
  `category` int COMMENT '模型类别, app:1|icon:2|detail:4')
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
;

CREATE TABLE IF NOT EXISTS `rp_dataengine.apppkg_icon2vec_par_wi`(
  `app_name` string COMMENT '应用名称',
  `image_name` string COMMENT '图片名称',
  `features` string COMMENT 'ICON特征')
COMMENT '游戏图标向量表'
PARTITIONED BY (
  `day` string COMMENT '入库日期',
  `source_type` string COMMENT '数据来源, taptap|dysy')
ROW FORMAT SERDE
'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
;

CREATE TABLE IF NOT EXISTS `rp_dataengine.apppkg_index_mapping_par`(
  `app_name` string COMMENT '应用名称',
  `index` int COMMENT '索引')
COMMENT 'appname索引表'
PARTITIONED BY (
  `day` string COMMENT '入库日期')
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
;

CREATE TABLE IF NOT EXISTS `rp_dataengine.apppkg_game_competion_data_opt`(
  `app_name` string COMMENT '应用名称',
  `comp_top30` array<string> COMMENT 'top30竞品')
COMMENT '游戏竞品结果表'
PARTITIONED BY (
  `day` string COMMENT '入库日期',
  `category` int COMMENT '模型类别, app:1|icon:2|detail:4|app&icon:3|app&detail:5|app&icon&detail:7')
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
;