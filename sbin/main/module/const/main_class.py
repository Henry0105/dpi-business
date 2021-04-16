# coding=utf-8

## 人群处理
# 二次过滤
CROWD_FILTER = "com.mob.dataengine.engine.core.crowd.CrowdFilter"
# 人群交并差
CROWD_SET = "com.mob.dataengine.engine.core.crowd.CrowdSet"
# 人群交并差V2
CROWD_SET_V2 = "com.mob.dataengine.engine.core.crowd.CrowdSetV2"
# 多维过滤
MULTIDIMENSIONAL_FILTER = "com.mob.dataengine.engine.core.crowd.MultidimensionalFilter"

## mapping
# id_mapping服务
ID_MAPPING = "com.mob.dataengine.engine.core.mapping.IdMapping"
ID_MAPPING_V2 = "com.mob.dataengine.engine.core.mapping.IdMappingV2"
ID_MAPPING_V3 = "com.mob.dataengine.engine.core.mapping.IdMappingV3"
ID_MAPPING_V4 = "com.mob.dataengine.engine.core.mapping.IdMappingV4"
ID_MAPPING_PHONE_BK = "com.mob.dataengine.utils.idmapping.IdMappingPhoneBk"
ID_MAPPING_PID_BK = "com.mob.dataengine.utils.idmapping.IdMappingPidBk"

#数据脱敏逆向
DATA_ENCRYPT_DECODING = "com.mob.dataengine.engine.core.mapping.dataprocessor.DataEncryptionDecodingLaunch"
DATA_ENCRYPT_DECODING_V2 = "com.mob.dataengine.engine.core.mapping.dataprocessor.DataEncryptionDecodingLaunchV2"
DATA_ENCRYPT_DECODING_V3 = "com.mob.dataengine.engine.core.mapping.dataprocessor.DataEncryptionDecodingLaunchV3"

# 地理围栏mapping
LOCATION_DEVICE_MAPPING = "com.mob.dataengine.engine.core.mapping.LocationDeviceMapping"
ID_MAPPING_TOOLS = "com.mob.dataengine.utils.idmapping.IdMappingTools"
ID_MAPPING_TOOLS_V2 = "com.mob.dataengine.utils.idmapping.IdMappingToolsV2"
ID_MAPPING_SEC_TOOLS = "com.mob.dataengine.utils.idmapping.IdMappingSecTools"
KAFKA_ID_MAPPING_TOOLS = "com.mob.dataengine.utils.idmapping.kafka.IdMappingTools"

## tags
TAGS_INTEGRATION_CHECKER = "com.mob.dataengine.utils.tags.TagsIntegrationChecker"

## lookalike
LOOKALIKE_DISTANCE_CALCULATION = "com.mob.dataengine.engine.core.lookalike.discal.LookalikeDistanceCalLaunch"
## 自定义群体画像
CROWD_PORTRAIT_CALCULATION = "com.mob.dataengine.engine.core.portrait.CrowdPortraitCalculation"
CROWD_PORTRAIT_ADJUSTER = "com.mob.dataengine.engine.core.portrait.CrowdPortraitAdjuster"
CROWD_PORTRAIT_ESTIMATION = "com.mob.dataengine.engine.core.portrait.CrowdPortraitEstimation"

## profile_cal
PROFILE_CAL_SCORE = "com.mob.dataengine.engine.core.profilecal.ProfileCalScore"
PROFILE_CAL_APP_INFO = "com.mob.dataengine.engine.core.profilecal.ProfileCalAppInfo"
PROFILE_CAL_APP_INFO_V2 = "com.mob.dataengine.engine.core.profilecal.ProfileCalAppInfoV2"
PROFILE_CAL_FREQUENCY = "com.mob.dataengine.engine.core.profilecal.ProfileCalHomeWorKFrequency"
PROFILE_CAL_SOURCE_FLOW = "com.mob.dataengine.engine.core.profilecal.ProfileCalSourceAndFlow"
PROFILE_CAL_MONOMER = "com.mob.dataengine.engine.core.profilecal.ProfileBatchMonomer"
PROFILE_CAL_GROUP = "com.mob.dataengine.engine.core.profilecal.ProfileCalGroup"
CARRIER_PROFILE = "com.mob.dataengine.engine.core.profilecal.external.CarrierProfile"
JIGUANT_PROFILE = "com.mob.dataengine.engine.core.profilecal.external.JiGuangProfile"
GETUI_PROFILE = "com.mob.dataengine.engine.core.profilecal.external.GeTuiProfile"
# 批量输出单体画像支持回溯
PROFILE_EXPORT_BT = "com.mob.dataengine.engine.core.profile.export.bt.Bootstrap"
POI_SEARCH = "com.mob.dataengine.engine.core.profilecal.poisearch.PoiSearch"
NATIONAL_VISITOR = "com.mob.dataengine.engine.core.profilecal.NationalVisitor"
PHONE_OPERATOR_MAPPING = "com.mob.dataengine.engine.core.mapping.PhoneOperatorMapping"
PID_OPERATOR_MAPPING = "com.mob.dataengine.engine.core.mapping.PidOperatorMapping"

# 数据清洗任务
DATA_CLEANING = "com.mob.dataengine.engine.core.crowd.DataCleaning"

# spark utils
SPARK_UTILS = "com.mob.dataengine.utils.SparkUtils"

TAGS_V2_GENERATOR = "com.mob.dataengine.utils.tags.DmTagsV2Generator"
TAGS_HFILE_GENERATOR = "com.mob.dataengine.utils.hbase.TagsHFileGenerator"
TAGS_HFILE_GENERATOR_BOOTSTRAP = "com.mob.dataengine.utils.hbase.TagsHFileGeneratorBootStrap"

# apppkg2vec
APPPKG2VEC = "com.mob.dataengine.engine.core.apppkg2vec.GameApppkgSimilarityCalculator"


# ios画像表
TAGS_PROFILE_CHECKER = "com.mob.dataengine.utils.iostags.IosTagsChecker"
TAGS_PROFILE_INFO_GENERATOR_INCR = "com.mob.dataengine.utils.iostags.IosTagsGeneratorIncr"
TAGS_PROFILE_INFO_GENERATOR_FULL = "com.mob.dataengine.utils.iostags.IosTagsGeneratorFull"


# 时间维度app状态筛选
CrowdAppTime = "com.mob.dataengine.utils.crowd.CrowdAppTime"
CrowdAppTimeFilter = "com.mob.dataengine.engine.core.crowd.CrowdAppTimeFilter"

# DPI
BUSINESS_DPI_MKT_URL = "com.mob.dataengine.engine.core.business.dpi.DpiMktUrl"
