package com.kakaopage.crm.extraction.spark

import com.kakaopage.crm.extraction.Predicate
import com.kakaopage.crm.extraction.predicates.Conjunction
import org.apache.spark.sql.DataFrame

import scala.collection.JavaConverters._

class ConjunctionExecutor extends PredicateExecutor[Conjunction] {

  override def execute(df: DataFrame, conjunction: Conjunction): DataFrame = {
    val predicates: Seq[Predicate] = conjunction.getPredicates.asScala
    df.filter(r => {
      true
    })

  }
}