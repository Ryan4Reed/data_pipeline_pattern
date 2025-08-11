package com.bruh.pipes.common.dq

import org.apache.spark.sql.{DataFrame, SparkSession}

object DQRunner {
  type ParamMap = Map[String, String]

  def validate(df: DataFrame, rules: Seq[DQRule], failFast: Boolean): Unit = {
    val results  = rules.map(_.apply(df))
    val failures = results.filterNot(_.ok)
    if (failures.nonEmpty && failFast) {
      val msg = failures.map(r => s"${r.name} details=${r.details}").mkString("; ")
      throw new IllegalStateException(s"DQ failed: $msg")
    }
  }

  def fromMetadata(entries: Seq[(String, ParamMap)]): Seq[DQRule] =
    entries.map { case (ruleType, params) =>
      ruleType match {
        case "filled_ratio" =>
          val col = required(params, "column")
          val min = required(params, "min").toDouble
          require(min >= 0.0 && min <= 1.0, s"filled_ratio.min must be in [0,1], got $min")
          FilledRatio(col, min)

        case "value_ratio" =>
          val col = required(params, "column")
          val op  = required(params, "op").toLowerCase
          val b   = required(params, "value").toDouble
          val min = required(params, "min").toDouble
          require(min >= 0.0 && min <= 1.0, s"value_ratio.min must be in [0,1], got $min")
          ValueRatio(col, op, b, min)

        case "max_percentile" =>
          val col = required(params, "column")
          val p   = required(params, "p").toDouble
          val mx  = required(params, "max").toDouble
          require(p > 0.0 && p <= 1.0, s"max_percentile.p must be in (0,1], got $p")
          MaxPercentile(col, p, mx)

        case other =>
          throw new IllegalArgumentException(s"Unknown DQ rule: $other")
      }
    }

  private def required(m: ParamMap, k: String): String =
    m.getOrElse(k, throw new IllegalArgumentException(s"Missing parameter '$k'"))
}
