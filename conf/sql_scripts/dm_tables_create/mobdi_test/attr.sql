CREATE TABLE mobdi_test.dim_pid_attribute_full_par_sec(
  pid_id string COMMENT '手机id',
  areacode string COMMENT '地区码',
  city string COMMENT '城市',
  company string COMMENT '运营商',
  zip string COMMENT '邮编',
  carrier string COMMENT '运营商',
  year string COMMENT '发布年份',
  province string COMMENT '省份中文名',
  province_code string COMMENT '省份代码',
  country string COMMENT '国家中文名',
  country_code string COMMENT '国家代码')
COMMENT '手机呼叫信息表'
PARTITIONED BY (
  day string COMMENT '日期',
  plat string COMMENT '平台')
stored as orc;