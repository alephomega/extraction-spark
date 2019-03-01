package com.kakaopage.crm.extraction.spark

import com.kakaopage.crm.extraction.Predicate
import com.kakaopage.crm.extraction.predicates.Equal
import org.apache.spark.sql.Column

object Predicates {

  val run: (Predicate) => Column = {
    case p: Equal => {
      Functions.invoke(p.firstOperand).equalTo(Functions.invoke(p.secondOperand))
    }
  }
}
