package com.bruh.pipes.common.logging

import org.slf4j.{Logger, LoggerFactory}

trait JobLogging {
  @transient protected lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  def logInfo(msg: String): Unit = logger.info(msg)
  def logWarn(msg: String): Unit = logger.warn(msg)
  def logError(msg: String, t: Throwable): Unit = logger.error(msg, t)
}
