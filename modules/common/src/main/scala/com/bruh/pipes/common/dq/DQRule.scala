package com.bruh.pipes.common.dq

import org.apache.spark.sql.{DataFrame, functions => F}

sealed trait DQRule { def name: String; def apply(df: DataFrame): DQResult }

final case class DQResult(name: String, passed: Boolean, details: Map[String,String] = Map.empty)

final case class NonZeroRatio(column: String, min: Double) extends DQRule {
  val name: String = s"non_zero_ratio($column)>=$min"
  def apply(df: DataFrame): DQResult = {
    val ratio = df.agg((F.sum(F.when(F.col(column) > 0, 1).otherwise(0)) / F.count(F.lit(1))).as("r"))
      .first.getDouble(0)
    DQResult(name, ratio >= min, Map("observed" -> ratio.toString))
  }
}

final case class MaxPercentile(column: String, p: Double) extends DQRule {
  val name: String = s"max_percentile($column)<=p$p"
  def apply(df: DataFrame): DQResult = {
    val q = df.stat.approxQuantile(column, Array(p), 1e-6).headOption.getOrElse(Double.NaN)
    DQResult(name, !q.isNaN, Map("approx_quantile" -> q.toString))
  }
}
