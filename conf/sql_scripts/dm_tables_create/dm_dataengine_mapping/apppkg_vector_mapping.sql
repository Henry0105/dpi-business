CREATE TABLE `dm_dataengine_mapping.apppkg_vector_mapping`
(
    `app_name`  string COMMENT '应用名称',
    `apppkg`    string COMMENT 'app包名',
    `icon_path` string COMMENT '图标对应的路径',
    `features`  array<double> COMMENT '稠密向量特征'
)
COMMENT 'appname特征表'
PARTITIONED BY (
    `day` string COMMENT '入库日期',
    `category` int COMMENT '模型类别, app:1|icon:2|detail:4')
ROW FORMAT SERDE
    'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
STORED AS INPUTFORMAT
    'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
    'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'