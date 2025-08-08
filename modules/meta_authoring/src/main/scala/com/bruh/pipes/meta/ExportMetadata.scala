package com.bruh.pipes.meta

import org.apache.spark.sql.SparkSession

object ExportMetadata {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.getOrCreate()
    import spark.implicits._
    spark.createDataset(ItemGroupRegistry.items).toDF
      .write.mode("overwrite").format("parquet") // swap to delta/iceberg in prod
      .saveAsTable("meta.item_defs_v1")
  }
}
