CREATE TABLE dm_dataengine_test.dm_tags_info_v2(
device string,
profile map<string,array<string>>)
PARTITIONED BY (
day string)
stored as orc;

create table dm_dataengine_test.ext_label_relation_full
(
value map<int, string> COMMENT '设备号 0:明文 1:md5加密 4:sha256加密',
type array<string> COMMENT '查询的设备类型类型',
label map<string,string> COMMENT '标签id和置信度的map'
)
PARTITIONED BY (
day string COMMENT '日期',
channel string COMMENT '数据来源渠道'
)
stored as orc;