package com.mob.dataengine.utils.iostags.beans

import com.mob.dataengine.utils.iostags.helper.TablePartitionsManager
import org.apache.spark.sql.SparkSession

/**
 * @author xlmeng
 */
case class QueryUnitContext(@transient spark: SparkSession, day: String,
                            tbManager: TablePartitionsManager, sample: Boolean = false, full: Boolean = false)
