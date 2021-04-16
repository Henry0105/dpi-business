create table dm_dataengine_test.dm_dataengine_code_mapping
(
 code string,
 en string comment '对应的英文含义/mobid',
 cn string comment '对应的中文含义',
 query string comment '查询条件,使用idType和encrypt'
)
PARTITIONED BY (
day string COMMENT '日期'
)
stored as orc;