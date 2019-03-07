package com.kakaopage.crm.extraction.spark

import com.kakaopage.crm.extraction.ra.{Relation, Selection}
import org.apache.spark.sql.DataFrame

object SelectionExecutor extends UnaryRelationalAlgebraOperatorExecutor[Selection] {
  def source(rel: Relation): DataFrame = ???

  override def execute(nv: Bag, selection: Selection, as: String): Bag = {
    val rel = selection.getRelation
    val condition = selection.getCondition

    Bag(source(rel).filter(Predicates.eval(condition, Seq.empty[Bag])), as)
  }
}
