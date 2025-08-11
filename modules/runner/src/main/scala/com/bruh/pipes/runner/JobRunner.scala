package com.bruh.pipes.runner

import org.apache.spark.sql.SparkSession
import com.bruh.pipes.common.{BaseJob, CommonMeta}
import com.bruh.pipes.common.logging.JobLogging

object JobRunner extends JobLogging {
  def run(job: BaseJob, rawArgs: Array[String]): Unit = {
    val args = rawArgs.sliding(2,2).collect { case Array(k,v) if k.startsWith("--") => k.drop(2) -> v }.toMap
    val runId = args.getOrElse("run_id", java.util.UUID.randomUUID().toString)
    val ds    = args.getOrElse("ds", java.time.LocalDate.now.toString)

    val spark = SparkSession.builder.appName(job.jobName).getOrCreate()
    val meta  = CommonMeta(job.domain, job.groupName, job.itemName, runId, ds, new java.sql.Timestamp(System.currentTimeMillis()))

    logInfo(s"Starting ${job.jobName} run_id=$runId ds=$ds table=${job.outputTable}")
    val inputs      = job.readSources(spark, args)
    val transformed = job.transform(inputs, spark, meta)
    job.write(transformed, spark, meta)
    logInfo(s"Completed ${job.jobName} run_id=$runId")
  }
}
