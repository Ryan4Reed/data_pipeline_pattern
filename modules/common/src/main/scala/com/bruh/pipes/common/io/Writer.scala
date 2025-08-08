package com.bruh.pipes.common.io

import org.apache.spark.sql.{DataFrame, SparkSession, functions => F}
import org.apache.spark.sql.functions.col

object Writer {
  def addIngestionMeta(df: DataFrame, runId: String): DataFrame =
    df.withColumn("_ingest_ts", F.current_timestamp())
      .withColumn("_run_id", F.lit(runId))

  def overwritePartitions(df: DataFrame, table: String)(implicit spark: SparkSession): Unit = {
    // Iceberg: df.writeTo(table).overwritePartitions()
    // Delta:   df.write.format("delta").mode("overwrite").saveAsTable(table)
    df.write.mode("overwrite").saveAsTable(table) // demo default (parquet/hive)
  }

  def mergeIntoPartitioned(df: DataFrame, table: String, keyCols: Seq[String])
                          (implicit spark: SparkSession): Unit = {
    val onExpr = keyCols.map(k => s"t.$k = s.$k").mkString(" AND ")
    df.createOrReplaceTempView("s")
    spark.table(table).createOrReplaceTempView("t")
    spark.sql(
      s"""
         |MERGE INTO $table t
         |USING s
         |ON $onExpr
         |WHEN MATCHED THEN UPDATE SET *
         |WHEN NOT MATCHED THEN INSERT *
         |""".stripMargin)
  }

  def dedupeByKeys(df: DataFrame, keys: Seq[String], orderByTs: String = "_ingest_ts"): DataFrame = {
    import org.apache.spark.sql.expressions.Window
    val w = Window.partitionBy(keys.map(col): _*).orderBy(col(orderByTs).desc)
    df.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") === 1).drop("_rn")
  }
}
