package com.kakaopage.crm.extraction.spark

import com.kakaopage.crm.extraction.Predicate
import org.apache.spark.sql.DataFrame


abstract class PredicateExecutor[T <: Predicate] {
  def execute(df: DataFrame, predicate: T): DataFrame
}

object PredicateExecutor {
  def executor(predicate: Predicate): PredicateExecutor[Predicate] = {
    null
  }
}
