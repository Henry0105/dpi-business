CREATE TABLE `rp_mobdi_app.apppkg_icon2vec_par_wi`
(
    `app_name`   string COMMENT '应用名称',
    `image_name` string COMMENT '图片名称',
    `features`   string COMMENT 'ICON特征'
)
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