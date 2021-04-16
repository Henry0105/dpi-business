CREATE TABLE `dm_sdk_mapping.city_level_mapping`(
  `city` string COMMENT '城市',
  `level` int COMMENT '城市等级')
stored as orc;

CREATE TABLE dm_sdk_mapping.mapping_area_par(
  country_s_code string COMMENT ' 国家标准码',
  province_s_code string COMMENT ' 省份标准码',
  city_s_code string COMMENT ' 城市标准码',
  area_s_code string COMMENT ' 区县标准码',
  country string COMMENT ' 国家中文名（mob使用）',
  province string COMMENT ' 省份中文名（mob使用）',
  city string COMMENT ' 城市中文名（mob使用）',
  country_code string COMMENT ' 国家代码（mob使用）',
  province_code string COMMENT ' 省份代码（mob使用）',
  city_code string COMMENT ' 城市代码（mob使用）',
  area_code string COMMENT ' 区县代码（mob使用）',
  country_poi string COMMENT ' 国家POI（外部交换）',
  province_poi string COMMENT ' 省份POI（外部交换）',
  city_poi string COMMENT ' 城市POI（外部交换）',
  area_poi string COMMENT ' 区县POI（外部交换）',
  provincial_capital string COMMENT '是否省会/直辖市',
  country_en string COMMENT '国家英文名',
  is_hot string COMMENT '未知',
  continents string COMMENT '国家所在洲',
  travel_area string COMMENT '国家所在大区',
  area string COMMENT '区中文名（mob使用）')
COMMENT '国家省市区代码对应中文'
PARTITIONED BY (
  flag string COMMENT '分区')
stored as orc;