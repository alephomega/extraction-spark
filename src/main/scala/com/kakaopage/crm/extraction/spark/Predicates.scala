package com.kakaopage.crm.extraction.spark

import com.kakaopage.crm.extraction.Predicate
import com.kakaopage.crm.extraction.predicates._
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

import scala.collection.JavaConverters._

object Predicates {

  val eval: (Predicate, Seq[Bag]) => Column = {
    case (p: Conjunction, s: Seq[Bag]) => {
      p.getPredicates.asScala.map(p => eval(p, s)).reduceLeft((r, c) => r.and(c))
    }

    case (p: Disjunction, s: Seq[Bag]) => {
      p.getPredicates.asScala.map(p => eval(p, s)).reduceLeft((r, c) => r.or(c))
    }

    case (p: Negation, s: Seq[Bag]) => {
      not(eval(p.getPredicate, s))
    }

    case (p: Equals, s: Seq[Bag]) => {
      Functions.column(p.firstOperand, s).equalTo(Functions.column(p.secondOperand, s))
    }

    case (p: GreaterThan, s: Seq[Bag]) => {
      Functions.column(p.firstOperand, s).gt(Functions.column(p.secondOperand, s))
    }

    case (p: GreaterThanOrEqualTo, s: Seq[Bag]) => {
      Functions.column(p.firstOperand, s).geq(Functions.column(p.secondOperand, s))
    }

    case (p: LessThan, s: Seq[Bag]) => {
      Functions.column(p.firstOperand, s).lt(Functions.column(p.secondOperand, s))
    }

    case (p: LessThanOrEqualTo, s: Seq[Bag]) => {
      Functions.column(p.firstOperand, s).leq(Functions.column(p.secondOperand, s))
    }

    case (p: Between, s: Seq[Bag]) => {
      Functions.column(p.firstOperand, s).between(Functions.column(p.secondOperand, s), Functions.column(p.thirdOperand, s))
    }

    case (p: IsIn[_], s: Seq[Bag]) => {
      Functions.column(p.getValue, s).isin(p.getElements.asScala)
    }
  }
}