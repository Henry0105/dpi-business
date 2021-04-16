package com.mob.dataengine.commons.utils

import java.io.InputStreamReader
import java.util.Properties

import org.apache.log4j.Logger

import scala.util.{Failure, Success, Try}

object PropUtils {
  private[this] lazy val logger: Logger = Logger.getLogger(PropUtils.getClass)

  private[this] lazy val prop: Properties = {
    val _prop = new Properties()
    val propFile = "hive_database_table.properties"
    Try {
      val propIn = new InputStreamReader(PropUtils.getClass.getClassLoader.getResourceAsStream(propFile), "UTF-8")
      _prop.load(propIn)
      propIn.close()
    } match {
      case Success(_) =>
        logger.info(s"props loaded succeed from [$propFile], {${_prop}}")
        _prop
      case Failure(ex) =>
        logger.error(ex.getMessage, ex)
        throw new InterruptedException(ex.getMessage)
    }
  }


  private[this] def getProperty(key: String): String = prop.getProperty(key)

  // 原始Android_full表
  lazy val HIVE_ORIGINAL_ANDROID_ID_MAPPING: String = getProperty("view_original_android_id_mapping")
  // 原始IOS_full表
  lazy val HIVE_ORIGINAL_IOS_ID_MAPPING: String = getProperty("table_original_ios_id_mapping")
  // 原始Android_full v2表
  lazy val HIVE_ORIGINAL_ANDROID_ID_MAPPING_V2: String = getProperty("view_original_android_id_mapping_v2")
  lazy val HIVE_ORIGINAL_ANDROID_ID_MAPPING_SEC: String = getProperty("view_original_android_id_mapping_sec")
  // 原始IOS_full v2表
  lazy val HIVE_ORIGINAL_IOS_ID_MAPPING_V2: String = getProperty("table_original_ios_id_mapping_v2")
  lazy val HIVE_ORIGINAL_IOS_ID_MAPPING_SEC: String = getProperty("table_original_ios_id_mapping_sec")
  // phone => device 回溯表
  lazy val HIVE_PHONE_DEVICE_TRACK_DF: String = getProperty("table_phone_device_track_df")
  lazy val HIVE_PID_DEVICE_TRACK_DF: String = getProperty("table_pid_device_track_df")


  // 活跃日志
  lazy val HIVE_TABLE_DEVICE_APP_RUNTIMES: String = getProperty("table_device_app_runtimes")
  // 客户活跃日志(ShareSDK)
  lazy val HIVE_TABLE_LOG_RUN_NEW: String = getProperty("table_log_run_new")
  // 客户活跃日志(公共库)
  lazy val HIVE_TABLE_PV: String = getProperty("table_pv")
  // 客户活跃日志(部分小米设备)
  lazy val HIVE_TABLE_XM_DEVICE_APP_RUNTIMES: String = getProperty("table_xm_device_app_runtimes")

  // 手机型号信息(中关村)
  lazy val HIVE_TABLE_PHONE_MODEL_INFO_ZGC: String = getProperty("table_phone_model_info_zgc")

  // 设备单体旅游标签日表
  lazy val HIVE_TABLE_TRAVEL_DAILY: String = getProperty("table_travel_daily")
  // 设备app在装列表
  lazy val HIVE_TABLE_MASTER_RESERVED_NEW: String = getProperty("table_master_reserved_new")
  // 设备app在装列表
  lazy val HIVE_TABLE_DEVICE_INSTALL_APP_MASTER_NEW: String = getProperty("table_device_install_app_master_new")
  // 活跃数据表(改进版,将pv.log和run.log合并成一张表)
  lazy val HIVE_TABLE_DEVICE_SDK_RUN_MASTER: String = getProperty("table_device_sdk_run_master")
  // 设备地理位置信息表
  lazy val HIVE_TABLE_DEVICE_STAYING_DAILY: String = getProperty("table_device_staying_daily")
  // 餐饮详情月表
  lazy val HIVE_TABLE_TIMEWINDOW_OFFLINE_PROFILE_V2: String = getProperty("table_timewindow_offline_profile_v2")
  lazy val HIVE_TABLE_PROFILE_HISTORY_INDEX: String = getProperty("table_profile_history_index")
  lazy val HIVE_TABLE_TIMEWINDOW_ONLINE_PROFILE_DAY: String = getProperty("table_timewindow_online_profile_day")
  // taglist/catelist回溯数据存放表
  lazy val HIVE_TABLE_TIMEWINDOW_ONLINE_PROFILE_DAY_V2: String = getProperty("table_timewindow_online_profile_day_v2")
  // ra标签
  lazy val HIVE_TABLE_TIMEWINDOW_ONLINE_PROFILE_DAY_V3: String = getProperty("table_timewindow_online_profile_day_v3")
  // 融慧产品1
  lazy val HIVE_TABLE_TIMEWINDOW_ONLINE_PROFILE_DAY_V4: String = getProperty("table_timewindow_online_profile_day_v4")
  // 融慧产品7
  lazy val HIVE_TABLE_TIMEWINDOW_ONLINE_PROFILE_RONGHUI_PRODUCT7: String =
    getProperty("table_timewindow_online_profile_ronghui_product7")
  // 融慧产品2
  lazy val HIVE_TABLE_TIMEWINDOW_ONLINE_PROFILE_RONGHUI_PRODUCT2: String =
    getProperty("table_timewindow_online_profile_ronghui_product2")
  // 捷信产品
  lazy val HIVE_TABLE_TIMEWINDOW_ONLINE_PROFILE_JX: String =
    getProperty("table_timewindow_online_profile_jx")
  lazy val HIVE_TABLE_TIMEWINDOW_ONLINE_PROFILE_DAY_V6: String = getProperty("table_timewindow_online_profile_day_v6")

  // 渠道清理
  lazy val HIVE_TABLE_APP_PKG_MAPPING_PAR: String = getProperty("table_app_pkg_mapping_par")
  // 餐饮店铺映射表
  lazy val HIVE_TABLE_CATERING_CATE_MAPPING: String = getProperty("table_catering_cate_mapping")
  // 城市等级映射表
  lazy val HIVE_TABLE_CITY_LEVEL_MAPPING: String = getProperty("table_city_level_mapping")
  // 渠道分析apppkg信息表
  lazy val HIVE_TABLE_APPPKG_INFO: String = getProperty("table_apppkg_info")
  // app分类表(人工分拣)
  lazy val HIVE_TABLE_APP_CATEGORY_MAPPING_PAR: String = getProperty("table_app_category_mapping_par")
  // tag分类表
  lazy val HIVE_TABLE_TAG_CAT_MAPPING_DMP_PAR: String = getProperty("table_tag_cat_mapping_dmp_par")
  // 设备在装列表
  lazy val HIVE_TABLE_DEVICE_APPLIST_NEW: String = getProperty("table_device_applist_new")
  // 媒介触达周表
  lazy val HIVE_TABLE_APP_ACTIVE_WEEKLY_PENETRANCE_RATIO: String =
    getProperty("table_app_active_weekly_penetrance_ratio")
  // 媒介触达月表
  lazy val HIVE_TABLE_APP_ACTIVE_MONTHLY_PENETRANCE_RATIO: String =
    getProperty("table_app_active_monthly_penetrance_ratio")
  // 最早出现时间
  lazy val HIVE_TABLE_DEVICE_MINTIME_MAPPING: String = getProperty("table_device_mintime_mapping")
  // device -> duid
  lazy val HIVE_TABLE_DEVICE_DUID_MAPPING_NEW: String = getProperty("table_device_duid_mapping_new")

  // 设备出境信息表
  lazy val HIVE_TABLE_RP_DEVICE_OUTING: String = getProperty("table_rp_device_outing")

  // 历史活跃信息表
  lazy val HIVE_TABLE_DEVICE_ACTIVE_APPLIST_FULL: String = getProperty("table_device_active_applist_full")
  lazy val HIVE_TABLE_DEVICE_ACTIVE_APPLIST: String = getProperty("table_device_active_applist")
  // 设备基础标签汇总表
  lazy val HIVE_TABLE_RP_DEVICE_PROFILE_FULL: String = getProperty("table_rp_device_profile_full")
  // 媒介画像全量表
  lazy val HIVE_TABLE_APP_INSTALL_PENETRANCE_RATIO: String = getProperty("table_app_install_penetrance_ratio")
  // 设备基础标签增量表
  lazy val HIVE_TABLE_RP_DEVICE_PROFILE_INCR: String = getProperty("table_rp_device_profile_incr")
  // apppkg周活跃表
  lazy val HIVE_TABLE_APP_ACTIVE_WEEKLY: String = getProperty("table_app_active_weekly")
  // 时间窗口线上标签
  lazy val HIVE_TABLE_TIMEWINDOW_ONLINE_PROFILE_V2: String = getProperty("table_timewindow_online_profile_v2")
  // 安卓标签活跃表
  lazy val HIVE_TABLE_RP_DEVICE_ACTIVE_LABEL_PROFILE: String = getProperty("table_rp_device_active_label_profile")
  // poi标签三个月数据表
  lazy val HIVE_TABLE_RP_DEVICE_LOCATION_3MONTHLY_HOMEWORK: String =
    getProperty("table_rp_device_location_3monthly_homework")
  // poi标签三个月数据表
  lazy val HIVE_TABLE_RP_DEVICE_FREQUENCY_3MONTHLY: String = getProperty("table_rp_device_frequency_3monthly")
  // Hive数据导入codis集群
  lazy val HIVE_TABLE_DEVICE_TAG_CODIS_FULL: String = getProperty("table_imei_device_tag_codis_full")

  // 要预测的device的临时表(用于快速恢复)
  lazy val HIVE_TABLE_DEVICE_UNINSTALL_PREDICTING_TMP: String = getProperty("table_device_uninstall_predicting_tmp")
  // dm_sdk_master.device_sdk_run_master14天数据汇总
  lazy val HIVE_TABLE_DEVICE_SDK_RUN_MASTER_2WEEKS: String = getProperty("table_device_sdk_run_master_2weeks")
  // 筛选的对卸载预测显著的pkg
  lazy val HIVE_TABLE_MODEL_DEVICE_TOP_APP4000: String = getProperty("table_model_device_top_app4000")
  // 要预测的device的特征表
  lazy val HIVE_TABLE_DEVICE_UNINSTALL_PREDICT_FEATURES: String =
    getProperty("table_device_uninstall_predict_features")
  // 预测的设备卸载概率详情
  lazy val HIVE_TABLE_DEVICE_UNINSTALL_PREDICTION: String = getProperty("table_device_uninstall_prediction")
  // 卸载预测统计表
  lazy val HIVE_TABLE_DEVICE_UNINSTALL_PREDICTION_STAT: String = getProperty("table_device_uninstall_prediction_stat")
  // apppkg分类与index(索引)映射表
  lazy val HIVE_TABLE_MAPPING_APP_CATEGORY_INDEX: String = getProperty("table_mapping_app_category_index")
  // appkey与pkg的对应表
  lazy val HIVE_TABLE_APPKEY_PKG_MAPPING_PAR: String = getProperty("table_appkey_pkg_mapping_par")
  // 给分析同学生成的feature表,用于训练模型
  lazy val HIVE_TABLE_FEATURES_TABLE: String = getProperty("table_features_table")

  // ********************************  rp_dataengine  *****************************************
  // 历史输入设备记录(旧) todo delete
  lazy val HIVE_TABLE_MOBEYE_O2O_LOG_CLEAN: String = getProperty("table_mobeye_o2o_log_clean")
  // 历史输入设备记录(新)
  lazy val HIVE_TABLE_DATA_OPT_CACHE: String = getProperty("table_data_opt_cache")
  lazy val HIVE_TABLE_DATA_OPT_CACHE_NEW: String = getProperty("table_data_opt_cache_new")
  lazy val HIVE_TABLE_DATA_HUB: String = getProperty("table_data_hub")
  lazy val HIVE_TABLE_DATAENGINE_CODE_MAPPING: String = getProperty("table_dataengine_code_mapping")
  // 历史设备记录表(自定义群体画像推算中间结果,含部分基础标签数据)
  lazy val HIVE_TABLE_CROWD_PORTRAIT_SOURCE_DEVCIE_PROFILE: String =
    getProperty("table_crowd_portrait_source_devcie_profile")
  // 自定义群体标签计算结果校准表
  lazy val HIVE_TABLE_CROWD_PORTRAIT_ADJUST_CALCULATION_SCORE: String =
    getProperty("table_crowd_portrait_calculation_adjust_score")
  // 自定义群体标签计算结果表
  lazy val HIVE_TABLE_CROWD_PORTRAIT_CALCULATION_SCORE: String = getProperty("table_crowd_portrait_calculation_score")
  // 自定义群体画像推算结果表
  lazy val HIVE_TABLE_CROWD_PORTRAIT_ESTIMATION_SCORE: String = getProperty("table_crowd_portrait_estimation_score")
  // 家居类别系数映射表(家居画像专用)
  lazy val HIVE_TABLE_HOME_IMPROVEMENT_COEFFICIENT: String = getProperty("table_home_improvement_coefficient")
  // 家居品牌系数映射表(家居画像专用)
  lazy val HIVE_TABLE_HOME_IMPROVEMENT_BRAND: String = getProperty("table_home_improvement_brand")
  // 服装子类别系数映射表(服装画像专用)
  lazy val HIVE_TABLE_CLOTHING_CATE_MAPPING: String = getProperty("table_clothing_cate_mapping")
  // 服装大类别系数映射表(服装画像专用)
  lazy val HIVE_TABLE_CLOTHING_MEGA_CATE_MAPPING: String = getProperty("table_clothing_mega_cate_mapping")
  // 城市/服装品牌映射表(服装画像专用)
  lazy val HIVE_TABLE_CITY_CATE_CLOTHING_BRAND_MAPPING: String = getProperty("table_city_cate_clothing_brand_mapping")
  // 城市等级/服装品牌映射表(服装画像专用)
  lazy val HIVE_TABLE_CITY_LEVEL_CATE_CLOTHING_BRAND_MAPPING: String =
    getProperty("table_city_level_cate_clothing_brand_mapping")
  // ios/adr系统调节系数映射表
  lazy val HIVE_TABLE_IOS_CONFIG: String = getProperty("table_ios_config")
  // lookalike相关(暂停,后期完善)
  lazy val HIVE_TABLE_RP_DATAENGINE_PCA_EXPLAINED_VARIANCE: String =
    getProperty("table_rp_dataengine_pca_explained_variance")
  // lookalike相关(暂停,后期完善)
  lazy val HIVE_TABLE_RP_DATAENGINE_PCA_MATRIX_PC: String = getProperty("table_rp_dataengine_pca_matrix_pc")
  // lookalike相关(暂停,后期完善)
  lazy val HIVE_TABLE_RP_DATAENGINE_PCA_TAGS_TFIDF_MAPPING: String =
    getProperty("table_rp_dataengine_pca_tags_tfidf_mapping")
  // lookalike相关
  lazy val HIVE_TABLE_RP_MOBEYE_TFIDF_PCA_TAGS_MAPPING: String = getProperty("table_rp_mobeye_tfidf_pca_tags_mapping")
  // lookalike相关
  lazy val HIVE_TABLE_RP_MOBEYE_TFIDF_PCA_DEMO: String = getProperty("table_rp_mobeye_tfidf_pca_demo")
  // device映射表
  lazy val HIVE_TABLE_DEVICE_ID_TAGS_MAPPING: String = getProperty("table_device_id_tags_mapping")
  //device映射表2
  lazy val HIVE_TABLE_DEVICE_ID_DATAENGINE_TAGS_MAPPING: String = getProperty("table_dataengine_device_id_tags_mapping")
  // id_mapping: device=>mac/imei/phone/imsi/idfa
  lazy val HIVE_TABLE_DM_DEVICE_MAPPING_SEC: String = getProperty("table_dm_device_mapping_sec")
  lazy val HIVE_TABLE_DM_DEVICE_MAPPING_SEC_VIEW: String = getProperty("table_dm_device_mapping_sec_view")
  lazy val HIVE_TABLE_DM_DEVICE_MAPPING_SEC_INC: String = getProperty("table_dm_device_mapping_sec_inc")
  // id_mapping: imei=>device
  lazy val HIVE_TABLE_DM_IEID_MAPPING: String = getProperty("table_dm_ieid_mapping")
  lazy val HIVE_TABLE_DM_IEID_MAPPING_VIEW: String = getProperty("table_dm_ieid_mapping_view")
  // id_mapping: mac=>device
  lazy val HIVE_TABLE_DM_MCID_MAPPING: String = getProperty("table_dm_mcid_mapping")
  lazy val HIVE_TABLE_DM_MCID_MAPPING_VIEW: String = getProperty("table_dm_mcid_mapping_view")
  // id_mapping: phone=>device
  lazy val HIVE_TABLE_DM_PID_MAPPING: String = getProperty("table_dm_pid_mapping")
  lazy val HIVE_TABLE_DM_PID_MAPPING_VIEW: String = getProperty("table_dm_pid_mapping_view")
  // id_mapping: imsi=>device
  lazy val HIVE_TABLE_DM_ISID_MAPPING: String = getProperty("table_dm_isid_mapping")
  lazy val HIVE_TABLE_DM_ISID_MAPPING_VIEW: String = getProperty("table_dm_isid_mapping_view")
  // id_mapping: idfa=>device
  lazy val HIVE_TABLE_DM_IFID_MAPPING: String = getProperty("table_dm_ifid_mapping")
  lazy val HIVE_TABLE_DM_IFID_MAPPING_VIEW: String = getProperty("table_dm_ifid_mapping_view")
  // id_mapping: serialno=>device
  lazy val HIVE_TABLE_DM_SNID_MAPPING: String = getProperty("table_dm_snid_mapping")
  lazy val HIVE_TABLE_DM_SNID_MAPPING_VIEW: String = getProperty("table_dm_snid_mapping_view")
  lazy val HIVE_TABLE_DM_OIID_MAPPING: String = getProperty("table_dm_oiid_mapping")
  lazy val HIVE_TABLE_DM_OIID_MAPPING_VIEW: String = getProperty("table_dm_oiid_mapping_view")
  lazy val HIVE_TABLE_DIM_IEID_TRANSFORM_FULL_PAR_SEC: String = getProperty("table_dim_ieid_transform_full_par_sec")
  lazy val HIVE_TABLE_DIM_IFID_TRANSFORM_FULL_PAR_SEC: String = getProperty("table_dim_ifid_transform_full_par_sec")
  lazy val HIVE_TABLE_DIM_PID_TRANSFORM_FULL_PAR_SEC: String = getProperty("table_dim_pid_transform_full_par_sec")
  // id_mapping external src: phone, imei, imsi etc.
  lazy val HIVE_TABLE_ID_MAPPING_EXTERNAL_SRC: String = getProperty("table_id_mapping_external_src")
  lazy val HIVE_TABLE_ID_MAPPING_SEC_EXTERNAL_SRC: String = getProperty("table_id_mapping_sec_external_src")
  lazy val HIVE_TABLE_ID_MAPPING_EXTERNAL_FULL_INC: String = getProperty("table_id_mapping_external_inc")
  lazy val HIVE_TABLE_ID_MAPPING_SEC_EXTERNAL_FULL_INC: String = getProperty("table_id_mapping_sec_external_inc")
  lazy val HIVE_TABLE_ID_MAPPING_EXTERNAL_FULL_INC_VIEW: String = getProperty("table_id_mapping_external_inc_view")
  lazy val HIVE_TABLE_ID_MAPPING_SEC_EXTERNAL_FULL_INC_VIEW: String =
    getProperty("table_id_mapping_sec_external_inc_view")
  // id_mapping external src: phone, imei, imsi etc.
  lazy val HIVE_TABLE_ID_MAPPING_EXTERNAL_FULL: String = getProperty("table_id_mapping_external_full")
  lazy val HIVE_TABLE_ID_MAPPING_SEC_EXTERNAL_FULL: String = getProperty("table_id_mapping_sec_external_full")
  lazy val HIVE_TABLE_ID_MAPPING_EXTERNAL_FULL_VIEW: String = getProperty("table_id_mapping_external_full_view")
  lazy val HIVE_TABLE_ID_MAPPING_SEC_EXTERNAL_FULL_VIEW: String = getProperty("table_id_mapping_sec_external_full_view")
  // id_mapping: device=>mac/imei/phone/imsi/idfa
  lazy val HIVE_TABLE_DM_DEVICE_MAPPING_V2: String = getProperty("table_dm_device_mapping_v2")
  // id_mapping: imei=>device
  lazy val HIVE_TABLE_DM_IMEI_MAPPING_V2: String = getProperty("table_dm_imei_mapping_v2")
  // id_mapping: imei14=>device
  lazy val HIVE_TABLE_DM_IMEI14_MAPPING_V2: String = getProperty("table_dm_imei14_mapping_v2")
  // id_mapping: mac=>device
  lazy val HIVE_TABLE_DM_MAC_MAPPING_V2: String = getProperty("table_dm_mac_mapping_v2")
  // id_mapping: phone=>device
  lazy val HIVE_TABLE_DM_PHONE_MAPPING_V2: String = getProperty("table_dm_phone_mapping_v2")
  // id_mapping: imsi=>device
  lazy val HIVE_TABLE_DM_IMSI_MAPPING_V2: String = getProperty("table_dm_imsi_mapping_v2")
  // id_mapping: idfa=>device
  lazy val HIVE_TABLE_DM_IDFA_MAPPING_V2: String = getProperty("table_dm_idfa_mapping_v2")
  // id_mapping: serialno=>device
  lazy val HIVE_TABLE_DM_SERIALNO_MAPPING_V2: String = getProperty("table_dm_serialno_mapping_v2")
  lazy val HIVE_TABLE_DM_DEVICE_MAPPING_V3: String = getProperty("table_dm_device_mapping_v3")
  lazy val HIVE_TABLE_DM_DEVICE_MAPPING_V3_INC: String = getProperty("table_dm_device_mapping_v3_inc")
  // id_mapping: imei=>device
  lazy val HIVE_TABLE_DM_IMEI_MAPPING_V3: String = getProperty("table_dm_imei_mapping_v3")
  // id_mapping: mac=>device
  lazy val HIVE_TABLE_DM_MAC_MAPPING_V3: String = getProperty("table_dm_mac_mapping_v3")
  // id_mapping: phone=>device
  lazy val HIVE_TABLE_DM_PHONE_MAPPING_V3: String = getProperty("table_dm_phone_mapping_v3")
  // id_mapping: oaid=>device
  lazy val HIVE_TABLE_DM_OAID_MAPPING_V3: String = getProperty("table_dm_oaid_mapping_v3")
  // id_mapping: imsi=>device
  lazy val HIVE_TABLE_DM_IMSI_MAPPING_V3: String = getProperty("table_dm_imsi_mapping_v3")
  // id_mapping: idfa=>device
  lazy val HIVE_TABLE_DM_IDFA_MAPPING_V3: String = getProperty("table_dm_idfa_mapping_v3")
  // id_mapping: serialno=>device
  lazy val HIVE_TABLE_DM_SERIALNO_MAPPING_V3: String = getProperty("table_dm_serialno_mapping_v3")
  // idmapping使用view
  lazy val HIVE_TABLE_DM_DEVICE_MAPPING_V3_VIEW: String = getProperty("table_dm_device_mapping_v3_view")
  // id_mapping: imei=>device
  lazy val HIVE_TABLE_DM_IMEI_MAPPING_V3_VIEW: String = getProperty("table_dm_imei_mapping_v3_view")
  // id_mapping: mac=>device
  lazy val HIVE_TABLE_DM_MAC_MAPPING_V3_VIEW: String = getProperty("table_dm_mac_mapping_v3_view")
  // id_mapping: phone=>device
  lazy val HIVE_TABLE_DM_PHONE_MAPPING_V3_VIEW: String = getProperty("table_dm_phone_mapping_v3_view")
  // id_mapping: oaid=>device
  lazy val HIVE_TABLE_DM_OAID_MAPPING_V3_VIEW: String = getProperty("table_dm_oaid_mapping_v3_view")
  // id_mapping: imsi=>device
  lazy val HIVE_TABLE_DM_IMSI_MAPPING_V3_VIEW: String = getProperty("table_dm_imsi_mapping_v3_view")
  // id_mapping: idfa=>device
  lazy val HIVE_TABLE_DM_IDFA_MAPPING_V3_VIEW: String = getProperty("table_dm_idfa_mapping_v3_view")
  // id_mapping: serialno=>device
  lazy val HIVE_TABLE_DM_SERIALNO_MAPPING_V3_VIEW: String = getProperty("table_dm_serialno_mapping_v3_view")
  lazy val HIVE_TALBE_ID_MAPPING_EXTERNAL_FULL: String = getProperty("table_id_mapping_external_full")
  lazy val HIVE_TABLE_ISP_MAPPING: String = getProperty("table_call_phone_property")
  lazy val HIVE_TABLE_MAPPING_AREA_PAR: String = getProperty("table_mapping_area_par")
  lazy val HIVE_TABLE_DIM_PID_ATTRIBUTE_FULL_PAR_SEC: String = getProperty("table_dim_pid_attribute_full_par_sec")
  // id_mapping: phone=>device 可回溯
  lazy val HIVE_TABLE_DM_PHONE_MAPPING_V3_BK: String = getProperty("table_dm_phone_mapping_v3_bk")
  lazy val HIVE_TABLE_DM_PHONE_MAPPING_V3_BK_VIEW: String = getProperty("table_dm_phone_mapping_v3_bk_view")
  lazy val HIVE_TABLE_DM_PID_MAPPING_BK: String = getProperty("table_dm_pid_mapping_v3_bk")
  lazy val HIVE_TABLE_DM_PID_MAPPING_BK_VIEW: String = getProperty("table_dm_pid_mapping_v3_bk_view")
  // device标签
  lazy val HIVE_TABLE_DM_DEVICE_PROFILE_INFO: String = getProperty("table_dm_device_profile_info")
  lazy val HIVE_TABLE_DM_DEVICE_PROFILE_INFO_METADATA: String = getProperty("table_dm_device_profile_info_metadata")
  // 商业地理区域app信息表
  lazy val HIVE_TABLE_APPINFO_DAILY: String = getProperty("table_appinfo_daily")
  lazy val HIVE_TABLE_APPINFO_DAILY_V2: String = getProperty("table_appinfo_daily_v2")
  // 商业地理用表
  lazy val HIVE_TABLE_MOBEYE_O2O_BASE_SCORE_DAILY: String = getProperty("table_mobeye_o2o_base_score_daily")
  // 商业地理用表
  lazy val HIVE_TABLE_MOBEYE_O2O_LBS_SOUREANDFLOW_DAILY: String = getProperty("table_mobeye_o2o_lbs_soureandflow_daily")
  lazy val HIVE_TABLE_MOBEYE_O2O_LBS_HOMEANDWORK_DAILY: String = getProperty("table_mobeye_o2o_lbs_homeandwork_daily")
  lazy val HIVE_TABLE_MOBEYE_O2O_LBS_FREQUENCY_DAILY: String = getProperty("table_mobeye_o2o_lbs_frequency_daily")
  // 商业地理用表
  lazy val HIVE_TABLE_DW_BASE_POI_L1_GEOHASH: String = getProperty("table_dw_base_poi_l1_geohash")
  // ios设备信息
  lazy val HIVE_TABLE_IDFA_DEVICE_INFO_FULL: String = getProperty("table_idfa_device_info_full")
  // ios地理位置信息
  lazy val HIVE_TABLE_IDFA_IP_LOCATION_FULL: String = getProperty("table_idfa_ip_location_full")
  // ios常驻地
  lazy val HIVE_TABLE_IOS_PERMANENT_PLACE: String = getProperty("table_ios_permanent_place")
  // ios社交信息
  lazy val HIVE_TABLE_IOS_SNS_INFO: String = getProperty("table_ios_sns_info")
  // ios活跃标签列表
  lazy val HIVE_TABLE_IOS_ACTIVE_TAG_LIST: String = getProperty("table_ios_active_tag_list")
  // ios居住地,工作地
  lazy val HIVE_TABLE_LOCATION_MONTHLY_IOS: String = getProperty("table_location_monthly_ios")

  lazy val HIVE_TABLE_DM_IDFA_PROFILE_INFO: String = getProperty("table_dm_idfa_profile_info")
  lazy val HIVE_TABLE_DM_IDFA_PROFILE_INFO_METADATA: String = getProperty("table_dm_idfa_profile_info_metadata")
  // 使用穷举生成的phone的明文与MD5对应关系
  lazy val HIVE_TABLE_TOTAL_PHONE_MD5_MAPPING: String = getProperty("table_total_phone_md5_mapping")
  lazy val HIVE_TABLE_TOTAL_MAC_MD5_MAPPING: String = getProperty("table_total_mac_md5_mapping")
  lazy val HIVE_TABLE_TOTAL_IMEI_MD5_MAPPING: String = getProperty("table_total_imei_md5_mapping")
  // 旅游出行偏好
  lazy val HIVE_TABLE_TRAVEL_LABEL_MONTHLY: String = getProperty("table_travel_label_monthly")
  // 模型置信度表
  lazy val HIVE_TABLE_DEVICE_MODELS_CONFIDENCE_FULL_VIEW: String =
    getProperty("table_device_models_confidence_full_view")
  // 新标签表
  lazy val HIVE_TABLE_DM_TAGS_INFO: String = getProperty("table_dm_tags_info")
  lazy val HIVE_TABLE_DM_TAGS_INFO_V2: String = getProperty("table_dm_tags_info_v2")
  lazy val HIVE_TABLE_DM_TAGS_INFO_VIEW: String = getProperty("table_dm_tags_info_view")
  lazy val HIVE_TABLE_SINGLE_PROFILE_INFO: String = getProperty("table_single_profile_info")
  lazy val HIVE_TABLE_GROUP_PROFILE_INFO: String = getProperty("table_group_profile_info")
  lazy val HIVE_TABLE_SINGLE_PROFILE_TRACK_INFO: String = getProperty("table_single_profile_track_info")
  // 全库标签表
  lazy val HIVE_TABLE_PROFILE_ALL_TAGS_INFO_DI: String = getProperty("table_profile_all_tags_info_di")
  lazy val HIVE_TABLE_PROFILE_ALL_TAGS_INFO_DI_V2: String = getProperty("table_profile_all_tags_info_di_v2")
  lazy val HIVE_TABLE_PROFILE_TAGS_INFO_FULL: String = getProperty("table_profile_tags_info_full")
  lazy val HIVE_TABLE_PROFILE_TAGS_INFO_FULL_V2: String = getProperty("table_profile_tags_info_full_v2")
  lazy val HIVE_TABLE_PROFILE_TAGS_INFO_FULL_VIEW: String = getProperty("table_profile_tags_info_full_view")
  lazy val HIVE_TABLE_PROFILE_TAGS_INFO_FULL_V2_VIEW: String = getProperty("table_profile_tags_info_full_v2_view")
  // 4张id画像表
  lazy val HIVE_TABLE_DM_DEVICE_TAGS_MAPPING: String = getProperty("table_dm_device_tags_mapping")
  lazy val HIVE_TABLE_DM_IMEI_TAGS_MAPPING: String = getProperty("table_dm_imei_tags_mapping")
  lazy val HIVE_TABLE_DM_MAC_TAGS_MAPPING: String = getProperty("table_dm_mac_tags_mapping")
  lazy val HIVE_TABLE_DM_PHONE_TAGS_MAPPING: String = getProperty("table_dm_phone_tags_mapping")

  lazy val HIVE_TABLE_DEVICE_PROFILE_FULL_ENHANCE: String = getProperty("table_dm_device_profile_full_enhance")
  lazy val HIVE_TABLE_DEVICE_PROFILE_FULL_ENHANCE_VIEW: String =
    getProperty("table_dm_device_profile_full_enhance_view")
  lazy val HIVE_TABLE_DM_IMEI_LATEST_TAGS_MAPPING: String = getProperty("table_dm_imei_latest_tags_mapping")
  lazy val HIVE_TABLE_DM_IEID_LATEST_TAGS_MAPPING: String = getProperty("table_dm_ieid_latest_tags_mapping")
  lazy val HIVE_TABLE_DM_IMEI_LATEST_TAGS_MAPPING_VIEW: String = getProperty("table_dm_imei_latest_tags_mapping_view")
  lazy val HIVE_TABLE_DM_IEID_LATEST_TAGS_MAPPING_VIEW: String = getProperty("table_dm_ieid_latest_tags_mapping_view")
  lazy val HIVE_TABLE_DM_DEVICE_LATEST_TAGS_MAPPING: String = getProperty("table_dm_device_latest_tags_mapping")
  lazy val HIVE_TABLE_DM_DEVICE_SEC_LATEST_TAGS_MAPPING: String = getProperty("table_dm_device_sec_latest_tags_mapping")
  lazy val HIVE_TABLE_DM_MAC_LATEST_TAGS_MAPPING: String = getProperty("table_dm_mac_latest_tags_mapping")
  lazy val HIVE_TABLE_DM_MCID_LATEST_TAGS_MAPPING: String = getProperty("table_dm_mcid_latest_tags_mapping")
  lazy val HIVE_TABLE_DM_PHONE_LATEST_TAGS_MAPPING: String = getProperty("table_dm_phone_latest_tags_mapping")
  lazy val HIVE_TABLE_DM_PID_LATEST_TAGS_MAPPING: String = getProperty("table_dm_pid_latest_tags_mapping")

  // 社交账号表
  lazy val HIVE_TABLE_RP_DEVICE_SNS_FULL: String = getProperty("table_rp_device_sns_full")
  // 社交账号到device的mapping表
  lazy val HIVE_TABLE_DM_SNSUID_DEVICE_MAPPING: String = getProperty("table_dm_snsuid_device_mapping")
  // apppkg2vec
  lazy val HIVE_TABLE_APPPKG_APP2VEC_PAR_WI: String = getProperty("table_apppkg_app2vec_par_wi")
  lazy val HIVE_TABLE_APPPKG_ICON2VEC_PAR_WI: String = getProperty("table_apppkg_icon2vec_par_wi")
  lazy val HIVE_TABLE_APPPKG_VECTOR_MAPPING: String = getProperty("table_apppkg_vector_mapping")
  lazy val HIVE_TABLE_RP_APP_NAME_INFO: String = getProperty("table_rp_app_name_info")
  lazy val HIVE_TABLE_APPPKG_INDEX_MAPPING_PAR: String = getProperty("table_apppkg_index_mapping_par")
  lazy val HIVE_TABLE_APP_DETAILS_SDK: String = getProperty("table_app_details_sdk")
  lazy val HIVE_TABLE_MOBSPIDER_TAPTAP_DETAIL: String = getProperty("table_mobspider_taptap_detail")
  lazy val HIVE_TABLE_DIYIYOU_GAME_DETAIL: String = getProperty("table_diyiyou_game_detail")
  lazy val HIVE_TABLE_APPPKG_GAME_COMPETION_DATA_OPT: String = getProperty("table_apppkg_game_competion_data_opt")
  lazy val HIVE_TABLE_APPPKG_TMP_APP2VEC_MAPPING: String = getProperty("table_apppkg_tmp_app2vec_mapping")

  // ext_label_merge
  lazy val HIVE_TABLE_LABEL_RELATION: String = getProperty("table_label_relation")
  lazy val HIVE_TABLE_EXT_LABEL_RELATION_FULL: String = getProperty("table_label_relation_full")

  // id/mac/idfa/pid 明文全量表
  lazy val HIVE_TABLE_IEID_FULL: String = getProperty("table_ieid_full")
  lazy val HIVE_TABLE_IFID_FULL: String = getProperty("table_ifid_full")
  lazy val HIVE_TABLE_PID_FULL: String = getProperty("table_pid_full")
  lazy val HIVE_TABLE_MCID_FULL: String = getProperty("table_mcid_full")
  lazy val HIVE_TABLE_ISID_FULL: String = getProperty("table_isid_full")
  lazy val HIVE_TABLE_SNID_FULL: String = getProperty("table_snid_full")
  lazy val HIVE_TABLE_OIID_FULL: String = getProperty("table_oiid_full")




  // ios画像表
  lazy val HIVE_TABLE_DM_PROFILE_IOS_TAGS_INFO_DI: String = getProperty("table_dm_profile_ios_tags_info_di")
  lazy val HIVE_TABLE_DM_PROFILE_IOS_TAGS_INFO_FULL: String = getProperty("table_dm_profile_ios_tags_info_full")
  lazy val HIVE_TABLE_DM_PROFILE_IOS_TAGS_INFO_DI_SEC: String = getProperty("table_dm_profile_ios_tags_info_di_sec")
  lazy val HIVE_TABLE_DM_PROFILE_IOS_TAGS_INFO_FULL_SEC: String = getProperty("table_dm_profile_ios_tags_info_full_sec")

  // 未清洗app包名与app名称的映射关系
  lazy val HIVE_TABLE_DM_PKG_NAME_MAPPING: String = getProperty("table_dm_pkg_name_mapping")

  // 时间维度筛选app人群表(一年)
  lazy val HIVE_TABLE_DM_DEVICE_APP_TIME_STATUS_FUll: String = getProperty("table_dm_device_app_time_status_full")

  // pid_xidLabel, pid_xid
  lazy val HIVE_TABLE_DM_DATAENGINE_PID_XIDLABEL: String = getProperty("table_pid_xidlabel")
  lazy val HIVE_TABLE_DM_DATAENGINE_PID_XID: String = getProperty("table_pid_xid")

  // DPI表
  lazy val HIVE_TABLE_RP_DPI_MKT_URL_INPUT: String = getProperty("hive_table_rp_dpi_mkt_url_input")
  lazy val HIVE_TABLE_RP_DPI_MKT_URL_WITHTAG: String = getProperty("hive_table_rp_dpi_mkt_url_withtag")
  lazy val HIVE_TABLE_RP_DPI_MKT_URL_TAG: String = getProperty("hive_table_rp_dpi_mkt_url_tag")
  lazy val HIVE_TABLE_RP_DPI_MKT_URL_MP: String = getProperty("hive_table_rp_dpi_mkt_url_mp")
  lazy val HIVE_TABLE_RP_DPI_MKT_URL_PRE_SCREEN: String = getProperty("hive_table_rp_dpi_mkt_url_pre_screen")

  lazy val HIVE_TABLE_RP_DPI_MKT_TAG_INIT: String = getProperty("hive_table_rp_dpi_mkt_tag_init")
  lazy val HIVE_TABLE_RP_DPI_DIM_DOMAIN: String = getProperty("hive_table_rp_dim_dpi_domain")

  lazy val HIVE_TABLE_RP_DPI_MARKETPLUS_TAG_URL_MAPPING: String =
    getProperty("hive_table_rp_dpi_marketplus_tag_url_mapping")
  lazy val HIVE_TABLE_RP_DPI_FIN_TAG_URL_MAPPING: String = getProperty("hive_table_rp_dpi_fin_tag_url_mapping")
  lazy val HIVE_TABLE_RP_DPI_MOBEYE_TAG_URL_MAPPING: String = getProperty("hive_table_rp_dpi_mobeye_tag_url_mapping")
  lazy val HIVE_TABLE_RP_DPI_GA_TAG_URL_MAPPING: String = getProperty("hive_table_rp_dpi_ga_tag_url_mapping")

}

