package com.bruh.pipes.common

import org.apache.spark.sql.{DataFrame, SparkSession}

trait BaseJob {
  def jobName: String
  def domain: String
  def itemName: String
  def outputTable: String
  def keyColumns: Seq[String]
  def partitionColumn: String = "ds"

  def readSources(spark: SparkSession, args: Map[String,String]): Map[String,DataFrame]
  def transform(inputs: Map[String,DataFrame], spark: SparkSession, meta: CommonMeta): DataFrame
  def write(df: DataFrame, spark: SparkSession, meta: CommonMeta): Unit
}

trait RecordJob extends BaseJob
trait EventStreamJob extends BaseJob
trait WideMetricJob extends BaseJob