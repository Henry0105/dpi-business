-- departed
CREATE TABLE `rp_dataengine.portrait_source_device_profile`(
  `device` string,
  `gender` int,
  `agebin` int,
  `income` int,
  `edu` int,
  `kids` int,
  `province_cn` string,
  `city_cn` string,
  `industry` int,
  `applist` string)
PARTITIONED BY (
  `uuid` string)
 STORED AS orc;

CREATE TABLE `rp_dataengine.home_improvement_coefficient`(
  `id` int COMMENT '主键',
  `type` int COMMENT '服装画像大类别',
  `sub_type` int COMMENT '服装画像子类别',
  `basis` double COMMENT '基础系数',
  `property_type` int COMMENT '基础标签类别',
  `property_coefficient` double COMMENT '基础标签类别系数',
  `property_code` int COMMENT '基础标签数值',
  `property_code_coefficient` double COMMENT '基础标签数值系数',
  `desc` string COMMENT '描述')
COMMENT '家居画像类别系数映射表'
STORED AS ORC;

CREATE TABLE `rp_dataengine.home_improvement_brand`(
  `id` int COMMENT '主键',
  `type` int COMMENT '服装画像类别id',
  `brand` string COMMENT '品牌名称',
  `min` double COMMENT '最小值',
  `max` double COMMENT '最大值',
  `desc` string COMMENT '描述',
  `cat1` string COMMENT '大类别',
  `cat2` string COMMENT '二级分类',
  `cat3` string COMMENT '三级分类',
  `price` double COMMENT '价格范围',
  `price_level` string COMMENT '价格等级',
  `price_level_code` int COMMENT '价格等级编码')
COMMENT '家居品牌系数映射表'
STORED AS ORC;

CREATE TABLE `rp_dataengine.clothing_mega_cate_mapping`(
  `agebin` int COMMENT '年龄标签',
  `gender` int COMMENT '性别标签',
  `occupation` int COMMENT '行业标签',
  `cate` string COMMENT '大类别',
  `weight` double COMMENT '加权系数')
COMMENT '服饰箱包画像大分类系数映射表'
STORED AS ORC;

CREATE TABLE `rp_dataengine.clothing_cate_mapping`(
  `agebin` int COMMENT '年龄标签',
  `gender` int COMMENT '性别标签',
  `occupation` int COMMENT '行业标签',
  `income` int COMMENT '收入标签',
  `cate1` string COMMENT '大类别',
  `cate2` string COMMENT '二级分类',
  `cate3` string COMMENT '三级分类',
  `price` string COMMENT '价格范围',
  `weight` double COMMENT '加权系数')
COMMENT '服饰箱包画像子类别系数映射表'
STORED AS ORC;


CREATE TABLE `rp_dataengine.city_level_cate_clothing_brand_mapping`(
  `cat2` string COMMENT '服装二级分类',
  `cat3` string COMMENT '服装三级分类',
  `price_level` string COMMENT '价格等级',
  `level` int COMMENT '城市等级',
  `province_cn` string COMMENT '省份(中文)',
  `province_code` string COMMENT '省份编码',
  `nameb` string COMMENT '品牌名称',
  `cnt` bigint COMMENT '计数',
  `percent` double COMMENT '占比')
STORED AS ORC;

CREATE TABLE `rp_dataengine.city_cate_clothing_brand_mapping`(
  `city` string COMMENT '城市名称',
  `cat2` string COMMENT '服装二级分类',
  `cat3` string COMMENT '服装三级分类',
  `price_level` string COMMENT '价格等级',
  `level` int COMMENT '城市等级',
  `province_cn` string COMMENT '省份(中文)',
  `city_code` string COMMENT '城市编码',
  `province_code` string COMMENT '省份编码',
  `nameb` string COMMENT '品牌名称',
  `cnt` bigint COMMENT '计数',
  `percent` double COMMENT '占比')
STORED AS ORC;

CREATE TABLE `rp_dataengine.crowd_portrait_estimation_score`(
  `label` string COMMENT '标签id',
  `type` int COMMENT '大类别',
  `sub_type` string COMMENT '子类别',
  `cnt` bigint COMMENT '计数',
  `percent` double COMMENT '求和')
COMMENT '自定义群体画像推算结果表'
PARTITIONED BY (
  `uuid` string)
STORED AS ORC;