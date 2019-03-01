package com.kakaopage.crm.extraction.spark

import com.kakaopage.crm.extraction
import com.kakaopage.crm.extraction.predicates.Equal
import org.apache.spark.sql.DataFrame

class EqualExecutor extends PredicateExecutor[Equal] {

  override def execute(df: DataFrame, p: Equal): DataFrame = {
    val _1 = p.firstOperand
    val _2 = p.secondOperand

    df.filter(Functions.invoke(p.asInstanceOf[extraction.Function]))
  }
}
