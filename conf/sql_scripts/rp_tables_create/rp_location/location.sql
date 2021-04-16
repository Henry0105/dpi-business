CREATE TABLE IF NOT EXISTS rp_mobdi_app.rp_device_location_3monthly(
  device string,
  lon_home string,
  lat_home string,
  cluster_home string,
  cnt_home string,
  max_distance_home string,
  min_distance_home string,
  confidence_home string,
  lon_work string,
  lat_work string,
  cluster_work string,
  cnt_work string,
  max_distance_work string,
  min_distance_work string,
  confidence_work string,
  type int,
  country_home string,
  province_home string,
  area_home string,
  country_cn_home string,
  province_cn_home string,
  area_cn_home string,
  country_work string,
  province_work string,
  area_work string,
  country_cn_work string,
  province_cn_work string,
  area_cn_work string,
  city_home string,
  city_cn_home string,
  city_work string,
  city_cn_work string)
PARTITIONED BY (
  day string)
  stored as orc;


CREATE TABLE IF NOT EXISTS rp_mobdi_app.rp_device_frequency_3monthly(
  device string COMMENT '设备device',
  lon double COMMENT '经度',
  lat double COMMENT '纬度',
  cnt bigint COMMENT '出现天数',
  country string COMMENT '国家',
  province string COMMENT '省份',
  city string COMMENT '城市',
  area string COMMENT '区域',
  rank int COMMENT '排名')
COMMENT '设备常去地（三个月）'
PARTITIONED BY (
  day string COMMENT '时间分区')
  stored as orc;