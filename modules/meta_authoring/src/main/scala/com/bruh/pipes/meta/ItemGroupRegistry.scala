package com.bruh.pipes.meta

final case class DQRuleSpec(ruleType: String, params: Map[String,String])
final case class ItemDef(
  domain: String,
  groupName: String,
  itemName: String,
  keyColumns: Seq[String],
  dqRules: Seq[DQRuleSpec],
  outputTable: String,
  shape: String  // "record" | "event" | "metrics"
)

object ItemGroupRegistry {
  val items: Seq[ItemDef] = Seq(
    ItemDef(
      domain = "core",
      groupName = "crm",
      itemName = "crm_users",
      keyColumns = Seq("user_id"),
      dqRules = Seq(
        DQRuleSpec("non_zero_ratio", Map("column" -> "user_id", "min" -> "0.999")),
        DQRuleSpec("non_zero_ratio", Map("column" -> "email", "min" -> "0.98"))
      ),
      outputTable = "lake.silver.core_crm_users",
      shape = "record"
    ),
    ItemDef(
      domain = "payments",
      groupName = "pos",
      itemName = "pos_transactions",
      keyColumns = Seq("transaction_id"),
      dqRules = Seq(
        DQRuleSpec("non_zero_ratio", Map("column" -> "amount", "min" -> "0.98")),
        DQRuleSpec("max_percentile", Map("column" -> "amount", "p" -> "0.999"))
      ),
      outputTable = "lake.silver.payments_pos_transactions",
      shape = "metrics"
    ),
    ItemDef(
      domain = "analytics",
      groupName = "web",
      itemName = "web_events",
      keyColumns = Seq("event_id"),
      dqRules = Seq(
        DQRuleSpec("non_zero_ratio", Map("column" -> "event_id", "min" -> "0.999"))
      ),
      outputTable = "lake.silver.analytics_web_events",
      shape = "event"
    )
  )
}
