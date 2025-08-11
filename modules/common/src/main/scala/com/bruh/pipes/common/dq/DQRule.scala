package com.bruh.pipes.common.dq

import org.apache.spark.sql.{DataFrame, functions => F}
import org.apache.spark.sql.types._

sealed trait DQRule {
  def name: String
  def apply(df: DataFrame): DQResult
}
final case class DQResult(name: String, ok: Boolean, details: Map[String, String] = Map.empty)

// % rows where column is "filled"
// - Strings: non-null and trimmed length > 0
// - Numerics: non-null and not NaN (for float/double)
// - Others: non-null
final case class FilledRatio(column: String, min: Double) extends DQRule {
  val name = s"filled_ratio($column)>=$min"
  def apply(df: DataFrame): DQResult = {
    val dt = df.schema(column).dataType
    val c  = F.col(column)
    val isFilled = dt match {
      case StringType =>
        c.isNotNull && F.length(F.trim(c)) > 0
      case DoubleType | FloatType =>
        c.isNotNull && !F.isnan(c.cast(DoubleType))
      case _ =>
        c.isNotNull
    }
    val ratio = df.agg((F.sum(F.when(isFilled, 1).otherwise(0)) / F.count(F.lit(1))).cast("double")).first.getDouble(0)
    DQResult(name, ratio >= min, Map("observed" -> ratio.toString))
  }
}

// % rows where numeric column is filled and satisfies a predicate (>, >=, <, <=, ==, !=)
final case class ValueRatio(column: String, op: String, bound: Double, min: Double) extends DQRule {
  val name = s"value_ratio($column $op $bound)>=$min"
  private def pred(colD: org.apache.spark.sql.Column) = op match {
    case "gt"  => colD >  bound
    case "gte" => colD >= bound
    case "lt"  => colD <  bound
    case "lte" => colD <= bound
    case "eq"  => colD === bound
    case "neq" => colD =!= bound
    case other => throw new IllegalArgumentException(s"Unsupported op '$other' (use gt,gte,lt,lte,eq,neq)")
  }
  def apply(df: DataFrame): DQResult = {
    val cD = F.col(column).cast(DoubleType)
    val isOk = cD.isNotNull && !F.isnan(cD) && pred(cD)
    val ratio = df.agg((F.sum(F.when(isOk, 1).otherwise(0)) / F.count(F.lit(1))).cast("double")).first.getDouble(0)
    DQResult(name, ratio >= min, Map("observed" -> ratio.toString))
  }
}

// Bound the p-quantile from above: approxQuantile(column, p) <= max
final case class MaxPercentile(column: String, p: Double, max: Double) extends DQRule {
  val name = s"max_percentile($column,p=$p)<=${max}"
  def apply(df: DataFrame): DQResult = {
    val arr = df.stat.approxQuantile(column, Array(p), 1e-6)
    val q = if (arr.isEmpty) Double.NaN else arr(0)
    val ok = !q.isNaN && q <= max
    DQResult(name, ok, Map("approx_quantile" -> q.toString))
  }
}
