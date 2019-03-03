package com.kakaopage.crm.extraction.spark

import com.kakaopage.crm.extraction.Predicate
import com.kakaopage.crm.extraction.predicates._
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

import scala.collection.JavaConverters._

object Predicates {

  val eval: (Predicate) => Column = {
    case p: Conjunction => {
      p.getPredicates.asScala.map(p => eval(p)).reduceLeft((r, c) => r.and(c))
    }

    case p: Disjunction => {
      p.getPredicates.asScala.map(p => eval(p)).reduceLeft((r, c) => r.or(c))
    }

    case p: Negation => {
      not(eval(p.getPredicate))
    }

    case p: Equals => {
      Functions.column(p.firstOperand).equalTo(Functions.column(p.secondOperand))
    }

    case p: GreaterThan => {
      Functions.column(p.firstOperand).gt(Functions.column(p.secondOperand))
    }

    case p: GreaterThanOrEqualTo => {
      Functions.column(p.firstOperand).geq(Functions.column(p.secondOperand))
    }

    case p: LessThan => {
      Functions.column(p.firstOperand).lt(Functions.column(p.secondOperand))
    }

    case p: LessThanOrEqualTo => {
      Functions.column(p.firstOperand).leq(Functions.column(p.secondOperand))
    }

    case p: In[_] => {
      Functions.column(p.getValue).isin(p.getElements.asScala)
    }
  }
}