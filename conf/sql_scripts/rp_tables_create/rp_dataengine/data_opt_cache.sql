CREATE TABLE IF NOT EXISTS `rp_dataengine.data_opt_cache` (
  data string comment 'sep: \u0001'
)
  COMMENT '设备数据缓存表,需要定期清理'
  PARTITIONED BY (
  `created_day` string COMMENT '创建时间',
  `biz` string comment '类型|data_type, 1：imei, 2：mac, 3：phone, 4:device, 5:imei_14',
  `uuid` string COMMENT '唯一标识外界指定'
) STORED AS orc;


CREATE TABLE IF NOT EXISTS rp_dataengine.data_opt_cache_new(
  id string COMMENT 'key (imei, mac, phone, device, imei_14)',
  match_ids map<int,string> COMMENT 'a string separated by: ',
  id_type int COMMENT '1: imei, 2: mac, 3: phone, 4: device, 5: imei_14',
  encrypt_type int COMMENT '0: not encrypted, 1: md5, 2: aes',
  data string)
PARTITIONED BY (
  day string,
  uuid string COMMENT 'identity a task')
  STORED AS orc;