package com.mob.dataengine.commons.enums

object JobName extends Enumeration {
  type JobName = Value

  val DEVICE_CLEAN = Value(1, "device_clean")
  val CALCULATION_PORTRAIT = Value(2, "calculation_portrait")
  val ID_MAPPING = Value(3, "id_mapping")
  val CROWD_PORTRAIT_CALCULATION = Value(4, "crowd_portrait_calculation")
  val CROWD_PORTRAIT_ESTIMATION = Value(5, "crowd_portrait_estimation")
  val CROWD_SELECTION_OPTIMIZATION = Value(6, "crowd_selection_optimization")
  val CLEANED_DEVICE_MAPPING_EXPORT = Value(7, "cleaned_device_mapping_export")
  val DEVICE_ID_MAPPING_HIVE_EXPORT = Value(8, "device_id_mapping_hive_export")
  val CROWD_SET_OPERATION = Value(9, "crowd_set_operation")
  val CROWD_FILTER = Value(10, "crowd_filter")
  val LOOKALIKE_NORMAL = Value(11, "lookalike_normal")
  val LOCATION_DEVICE_MAPPING = Value(12, "location_device_mapping")
  val PROFILE_CAL_SCORE = Value(13, "profile_cal_score")
  val PROFILE_CAL_APP_INFO = Value(14, "profile_cal_app_info")
  val PROFILE_CAL_FREQUENCY = Value(15, "profile_cal_frequency")
  val PROFILE_CAL_SOURCE_FLOW = Value(16, "profile_cal_source_flow")
  val PROFILE_EXPORT_BACK_TRACK = Value(17, "profile_export_back_track")// 回溯ProfileBatchBackTracker
  val DATA_ENCRYPT_DECODE = Value(18, "data_encrypt_parse")
  val ID_MAPPING_V2 = Value(19, "id_mapping_v2")
  val ID_MAPPING_V3 = Value(20, "id_mapping_v3")
  val PROFILE_CAL_BATCH_MONOMER = Value(21, "profile_cal_batch_monomer")
  val CROWD_SET_OPERATION_V2 = Value(22, "crowd_set_operation_v2")
  val MULTIDIMENSIONAL_FILTER = Value(23, "multidimensional_filter")
  val DATA_ENCRYPT_DECODE_V2 = Value(24, "data_encrypt_parse_v2")
  val LOCATION_DEVICE_MAPPING_V2 = Value(25, "location_device_mapping_v2")
  val DATA_CLEANING = Value(26, "data_cleaning")
  val CROWD_APPTIME_FILTER = Value(27, "crowd_app_time_filter")

  def getId(key: String): Int = {
    JobName.withName(key).id
  }
}
