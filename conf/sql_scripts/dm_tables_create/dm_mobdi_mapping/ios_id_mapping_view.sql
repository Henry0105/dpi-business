CREATE TABLE dm_mobdi_mapping.ios_id_mapping_view(
 device string,
 idfa string,
 idfa_tm string,
 idfa_ltm string,
 mac string,
 mac_tm string,
 phone string,
 phone_tm string,
 phone_ltm string)
COMMENT '测试数据'
PARTITIONED BY (
  `version` string COMMENT '版本')
  stored as orc;
CREATE TABLE dm_mobdi_mapping.ios_id_mapping_full_view(
 device string,
 idfa string,
 idfa_tm string,
 idfa_ltm string,
 mac string,
 mac_tm string,
 mac_ltm string,
 phone string,
 phone_tm string,
 phone_ltm string)
COMMENT '测试数据'
PARTITIONED BY (
  `version` string COMMENT '版本');