CREATE TABLE `dm_dataengine_tags.dm_device_latest_tags_mapping_view`(
  `device` string COMMENT '设备ID',
  `mac` string COMMENT 'mac地址',
  `imei` string COMMENT 'imei地址',
  `phone` string COMMENT '手机号',
  `country` string COMMENT '国家(编码)',
  `province` string COMMENT '省份(编码)',
  `city` string COMMENT '城市(编码)',
  `gender` int COMMENT '性别',
  `agebin` int COMMENT '年龄',
  `segment` int COMMENT '人群',
  `edu` int COMMENT '教育',
  `kids` int COMMENT '有无小孩标签',
  `income` int COMMENT '收入标签',
  `cell_factory` string COMMENT '设备品牌',
  `model` string COMMENT '设备型号',
  `model_level` int COMMENT '设备档次',
  `carrier` string COMMENT '运营商',
  `network` string COMMENT '网络',
  `screensize` string COMMENT '屏幕分辨率',
  `sysver` string COMMENT '操作系统版本',
  `tot_install_apps` int COMMENT '安装app个数',
  `country_cn` string COMMENT '国家(中文)',
  `province_cn` string COMMENT '省份(中文)',
  `city_cn` string COMMENT '城市(中文)',
  `city_level` int COMMENT '城市等级',
  `permanent_country` string COMMENT '常驻国家(编码)',
  `permanent_province` string COMMENT '常驻省份(编码)',
  `permanent_city` string COMMENT '常驻城市(编码)',
  `occupation` int COMMENT '职业',
  `house` int COMMENT '房产数',
  `repayment` int COMMENT '偿还能力',
  `car` int COMMENT '汽车数',
  `workplace` string COMMENT '工作地',
  `residence` string COMMENT '常驻地',
  `married` int COMMENT '婚姻状态',
  `applist` string COMMENT 'app安装列表',
  `permanent_country_cn` string COMMENT '常驻国家(中文)',
  `permanent_province_cn` string COMMENT '常驻省份（中文）',
  `permanent_city_cn` string COMMENT '常驻城市(中文)',
  `public_date` string COMMENT '手机发布时间',
  `consum_level` int COMMENT '消费水平',
  `life_stage` string COMMENT '人生阶段',
  `industry` int COMMENT '行业标签',
  `identity` string COMMENT '特殊身份',
  `special_time` string COMMENT '特殊时期属性',
  `processtime` string COMMENT '时间',
  `agebin_1001` int COMMENT '细分年龄标签',
  `nationality` string COMMENT '国籍（编码）',
  `nationality_cn` string COMMENT '国籍（中文）',
  `breaked` string COMMENT '是否越狱',
  `last_active` string COMMENT '设备最后活跃时间',
  `group_list` string COMMENT '人群细分标签',
  `city_level_1001` int COMMENT '新1线城市',
  `first_active_time` string COMMENT '设备首次出现时间',
  `price` string COMMENT '设备价格',
  `catelist` string COMMENT '在装app的类别列表',
  `tag_list` string COMMENT '标签列表',
  `frequency_geohash_list` string COMMENT '常去地geohash码',
  `home_geohash` string COMMENT '居住地geohash码',
  `work_geohash` string COMMENT '工作地geohash码',
  `match_flag` int COMMENT '是否匹配上')
PARTITIONED BY (
  `version` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
;