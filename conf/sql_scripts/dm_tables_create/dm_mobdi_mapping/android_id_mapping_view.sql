CREATE TABLE `dm_mobdi_mapping.android_id_mapping_view`(
  device string,
  imei string,
  imei_tm string,
  imei_ltm string,
  mac string,
  mac_tm string,
  mac_ltm string,
  phone string,
  phone_tm string,
  phone_ltm string,
  imsi string,
  imsi_tm string,
  imsi_ltm string,
  serialno string,
  serialno_tm string,
  serialno_ltm string)
COMMENT '安卓id mapping全量表'
PARTITIONED BY (`version` string)
stored as orc;
CREATE TABLE `dm_mobdi_mapping.android_id_mapping_full_view`(
  device string,
  imei string,
  imei_tm string,
  imei_ltm string,
  mac string,
  mac_tm string,
  mac_ltm string,
  phone string,
  phone_tm string,
  phone_ltm string,
  imsi string,
  imsi_tm string,
  imsi_ltm string,
  serialno string,
  serialno_tm string,
  serialno_ltm string,
  orig_imei string,
  orig_imei_tm string,
  orig_imei_ltm string,
  oaid string,
  oaid_tm string,
  oaid_ltm string)
COMMENT '安卓id mapping全量表'
PARTITIONED BY (`version` string);