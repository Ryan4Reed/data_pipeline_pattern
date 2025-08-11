package com.bruh.pipes.common.meta

import org.apache.spark.sql.SparkSession

final case class DQRuleRow(ruleType: String, params: Map[String,String])
final case class ItemDefRow(
  domain: String,
  groupName: String,
  itemName: String,
  keyColumns: Seq[String],
  dqRules: Seq[DQRuleRow],
  outputTable: String
)

object MetadataRepo {
  def load(spark: SparkSession, domain: String, itemName: String): ItemDefRow = {
    import spark.implicits._
    spark.table("meta.item_defs_v1").as[ItemDefRow]
      .filter(r => r.domain == domain && r.itemName == itemName)
      .head
  }
}
