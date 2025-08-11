package com.bruh.pipes.jobs

import org.apache.spark.sql.{DataFrame, SparkSession, functions => F}
import org.apache.spark.sql.types._
import com.bruh.pipes.common.{CommonMeta, BaseJob}
import com.bruh.pipes.common.logging.JobLogging
import com.bruh.pipes.common.io.Writer
import com.bruh.pipes.common.dq.{DQRunner}
import com.bruh.pipes.common.meta.MetadataRepo

object WebEventsJob extends BaseJob with JobLogging {
  override val jobName     = "WebEventsJob"
  override val domain      = "analytics"
  override val itemName    = "web_events"
  override val outputTable = "lake.silver.analytics_web_events"
  override val keyColumns  = Seq("event_id")

  private val schema = StructType(Seq(
    StructField("event_id", StringType, nullable = false),
    StructField("user_id", StringType, nullable = true),
    StructField("ts", StringType, nullable = true),
    StructField("event_type", StringType, nullable = true),
    StructField("props", StringType, nullable = true)
  ))

  def readSources(spark: SparkSession, args: Map[String,String]) = {
    val input = args.getOrElse("inputPath", "/mnt/data/web_events.csv")
    val df = spark.read.option("header","true").schema(schema).csv(input)
    Map("raw" -> df)
  }

  def transform(inputs: Map[String,DataFrame], spark: SparkSession, meta: CommonMeta): DataFrame = {
    val raw = inputs("raw")
    raw.select(
      F.col("event_id"),
      F.col("user_id"),
      F.col("event_type"),
      F.to_date(F.col("ts")).as("ds"),
      F.col("props")
    )
  }

  def write(df: DataFrame, spark: SparkSession, meta: CommonMeta): Unit = {
    val item   = MetadataRepo.load(spark, domain, itemName)
    val rules  = DQRunner.fromMetadata(item.dqRules.map(r => r.ruleType -> r.params))
    DQRunner.validate(df, rules, failFast = true)

    val staged = Writer.addIngestionMeta(df, meta.runId)
    val dedup  = Writer.dedupeByKeys(staged, keyColumns)
    Writer.mergeIntoPartitioned(dedup, outputTable, keyColumns)(spark)
  }

  def main(args: Array[String]): Unit =
    com.bruh.pipes.runner.JobRunner.run(this, args)
}
