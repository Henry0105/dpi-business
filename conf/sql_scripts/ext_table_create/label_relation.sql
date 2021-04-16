CREATE TABLE `dw_ext_exchange.lable_relation`
(
    `value`   string COMMENT '设备号',
    `type`    string COMMENT '查询的设备类型类型(phone)imeimd5,macmd5,imsimd5,phonemd5,idfa',
    `data` array<map<string,string>> COMMENT '标签',
    `channel` string COMMENT '渠道类型 aurora getui'
)
COMMENT '个推、极光查询获得的标签数据经过翻译后的数据'
PARTITIONED BY (
    `day` string COMMENT '入库日期')
ROW FORMAT SERDE
    'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
    'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
    'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'