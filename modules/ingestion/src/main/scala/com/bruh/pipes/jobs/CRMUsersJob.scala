// modules/jobs/src/main/scala/com/bruh/pipes/jobs/CRMUsersJob.scala
package com.bruh.pipes.jobs

import org.apache.spark.sql.{DataFrame, SparkSession, functions => F}
import org.apache.spark.sql.types._
import com.bruh.pipes.common.{CommonMeta, BaseJob}
import com.bruh.pipes.common.logging.JobLogging
import com.bruh.pipes.common.io.Writer
import com.bruh.pipes.common.dq.DQRunner
import com.bruh.pipes.common.meta.MetadataRepo

object CRMUsersJob extends BaseJob with JobLogging {
  override val jobName     = "CRMUsersJob"
  override val domain      = "core"
  override val itemName    = "crm_users"
  override val outputTable = "lake.silver.core_crm_users"
  override val keyColumns  = Seq("user_id")

  private val schema = StructType(Seq(
    StructField("user_id", StringType, nullable = false),
    StructField("email", StringType, nullable = true),
    StructField("country", StringType, nullable = true),
    StructField("created_at", StringType, nullable = true)
  ))

  def readSources(spark: SparkSession, args: Map[String,String]): Map[String,DataFrame] = {
    val input = args.getOrElse("inputPath", "/mnt/data/crm_users.csv")
    val df = spark.read.option("header","true").schema(schema).csv(input)
    Map("raw" -> df)
  }

  def transform(inputs: Map[String,DataFrame], spark: SparkSession, meta: CommonMeta): DataFrame = {
    val raw   = inputs("raw")
    val dsCol = F.coalesce(F.to_date(F.col("created_at")), F.to_date(F.lit(meta.ds)))
    raw.withColumn("ds", dsCol)
       .select("user_id", "email", "country", "ds")
  }

  def write(df: DataFrame, spark: SparkSession, meta: CommonMeta): Unit = {
    val item  = MetadataRepo.load(spark, domain, itemName)
    val rules = DQRunner.fromMetadata(item.dqRules.map(r => r.ruleType -> r.params))
    DQRunner.validate(df, rules, failFast = true)

    val staged = Writer.addIngestionMeta(df, meta.runId)
    val dedup  = Writer.dedupeByKeys(staged, keyColumns)

    val pc   = partitionColumn // "ds" by default
    val pval = meta.ds
    val ready = dedup.withColumn(pc, F.lit(pval))
    val singlePartition = ready.filter(F.col(pc) === F.lit(pval))

    Writer.mergeIntoPartitioned(
      df              = singlePartition,
      targetTable     = outputTable,
      keyColumns      = keyColumns,
      partitionColumn = pc,
      partitionValue  = pval
    )(spark)
  }

  def main(args: Array[String]): Unit =
    com.bruh.pipes.runner.JobRunner.run(this, args)
}
