package com.kakaopage.crm.extraction.spark

import com.kakaopage.crm.extraction.predicates.Disjunction
import org.apache.spark.sql.DataFrame

class DisjunctionExecutor extends PredicateExecutor[Disjunction] {
  override def execute(df: DataFrame, predicate: Disjunction): DataFrame = {

    null
  }
}
