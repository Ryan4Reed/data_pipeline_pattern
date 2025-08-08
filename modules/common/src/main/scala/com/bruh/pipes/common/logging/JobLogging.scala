package com.bruh.pipes.common.logging
import org.apache.log4j.{LogManager, Logger}

trait JobLogging {
  @transient protected lazy val logger: Logger = LogManager.getLogger(this.getClass)
  def logInfo(msg: String): Unit = logger.info(msg)
  def logWarn(msg: String): Unit = logger.warn(msg)
  def logError(msg: String, t: Throwable): Unit = logger.error(msg, t)
}