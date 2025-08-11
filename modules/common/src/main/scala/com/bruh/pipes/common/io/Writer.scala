// modules/common/src/main/scala/com/bruh/pipes/common/io/Writer.scala
package com.bruh.pipes.common.io

import org.apache.spark.sql.{DataFrame, SparkSession, functions => F}
import org.apache.spark.sql.functions.col
import java.util.UUID

object Writer {

  def addIngestionMeta(df: DataFrame, runId: String): DataFrame =
    df.withColumn("_ingest_ts", F.current_timestamp())
      .withColumn("_run_id", F.lit(runId))

  /** Overwrite exactly one partition (choose the block for your format). */
  def overwritePartition(
      df: DataFrame,
      targetTable: String,
      partitionColumn: String,
      partitionValue: String
  )(implicit spark: SparkSession): Unit = {
    val src = df.filter(F.col(partitionColumn) === F.lit(partitionValue))

    // ---- Delta Lake (uncomment if your table is Delta) ----
    // src.write
    //   .format("delta")
    //   .mode("overwrite")
    //   .option("replaceWhere", s"$partitionColumn = '$partitionValue'")
    //   .saveAsTable(targetTable)

    // ---- Apache Iceberg (uncomment if using Iceberg) ----
    // src.writeTo(targetTable).overwritePartitions()

    // ---- Plain Parquet/Hive fallback (dynamic partition overwrite) ----
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    src.write.mode("overwrite").insertInto(targetTable)
  }

  /** MERGE only the specified partition (requires Delta/Iceberg/Hudi). */
  def mergeIntoPartitioned(
      df: DataFrame,
      targetTable: String,
      keyColumns: Seq[String],
      partitionColumn: String,
      partitionValue: String
  )(implicit spark: SparkSession): Unit = {
    // Enforce single partition in the source
    val src = df.filter(F.col(partitionColumn) === F.lit(partitionValue))

    // Unique temp view to avoid collisions
    val srcView = s"_src_${UUID.randomUUID().toString.replace("-", "")}"
    src.createOrReplaceTempView(srcView)

    // ON: match keys AND the specific partition; use null-safe eq for keys
    val keysEq = keyColumns.map(k => s"s.$k <=> t.$k").mkString(" AND ")
    val onCond = s"$keysEq AND t.$partitionColumn = '$partitionValue'"

    // Exclude keys and partition from UPDATE set
    val updatable =
      src.columns.filterNot(c => keyColumns.contains(c) || c == partitionColumn)

    val setClause =
      if (updatable.nonEmpty) updatable.map(c => s"t.$c = s.$c").mkString(", ")
      else s"t.$partitionColumn = t.$partitionColumn" // no-op update

    val insertCols   = src.columns.mkString(", ")
    val insertValues = src.columns.map(c => s"s.$c").mkString(", ")

    val sql =
      s"""
         |MERGE INTO $targetTable t
         |USING $srcView s
         |ON $onCond
         |WHEN MATCHED THEN UPDATE SET $setClause
         |WHEN NOT MATCHED THEN INSERT ($insertCols) VALUES ($insertValues)
         |""".stripMargin

    spark.sql(sql)
    spark.catalog.dropTempView(srcView)
  }

  def dedupeByKeys(
      df: DataFrame,
      keys: Seq[String],
      orderByTs: String = "_ingest_ts"
  ): DataFrame = {
    import org.apache.spark.sql.expressions.Window
    val w = Window.partitionBy(keys.map(col): _*).orderBy(col(orderByTs).desc)
    df.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") === 1).drop("_rn")
  }
}
