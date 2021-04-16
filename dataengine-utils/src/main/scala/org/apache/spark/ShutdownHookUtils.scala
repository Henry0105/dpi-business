package org.apache.spark

import org.apache.spark.util.ShutdownHookManager

object ShutdownHookUtils {
  def addShutdownHook(hook: () => Unit): AnyRef = {
    ShutdownHookManager.addShutdownHook(hook)
  }

  def addShutdownHook(priority: Int)(hook: () => Unit): AnyRef = {
    ShutdownHookManager.addShutdownHook(priority)(hook)
  }
}
