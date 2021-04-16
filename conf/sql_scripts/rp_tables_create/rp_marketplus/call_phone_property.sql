CREATE TABLE IF NOT EXISTS rp_marketplus_wisdom.call_phone_property(
  id int COMMENT '号码前七位',
  isp string COMMENT '运营商',
  isp_code int COMMENT '运营商代码',
  city_name string COMMENT '城市名称',
  city_code string COMMENT '城市代码'
  )
PARTITIONED BY (
  day string
)
STORED AS orc;