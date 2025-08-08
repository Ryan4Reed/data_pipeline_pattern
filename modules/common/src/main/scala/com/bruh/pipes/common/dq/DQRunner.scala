package com.bruh.pipes.common.dq

import org.apache.spark.sql.DataFrame

object DQRunner {
  def fromMetadata(spec: Seq[(String, Map[String,String])]): Seq[DQRule] =
    spec.map {
      case ("non_zero_ratio", m) => NonZeroRatio(m("column"), m("min").toDouble)
      case ("max_percentile", m) => MaxPercentile(m("column"), m("p").toDouble)
      case (other, _)            => throw new IllegalArgumentException(s"Unknown DQ rule: $other")
    }

  def validate(df: DataFrame, rules: Seq[DQRule], failFast: Boolean = true): Seq[DQResult] = {
    val results = rules.map(_.apply(df))
    val failed  = results.filterNot(_.passed)
    if (failFast && failed.nonEmpty)
      throw new RuntimeException("DQ failed: " + failed.map(_.name).mkString(", "))
    results
  }
}
