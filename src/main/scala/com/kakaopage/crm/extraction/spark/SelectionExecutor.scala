package com.kakaopage.crm.extraction.spark

import com.kakaopage.crm.extraction.ra.{Relation, Selection}
import org.apache.spark.sql.DataFrame

object SelectionExecutor extends UnaryRelationalAlgebraOperatorExecutor[Selection] {

  def source(rel: Relation): DataFrame = {
    Metadata.source(rel.getName) match {
      case Some(s) => s.load()
      case _ => null
    }
  }

  override def execute(empty: Bag, selection: Selection, as: String): Bag = {
    val rel = selection.getRelation
    val condition = selection.getCondition

    Bag(source(rel).filter(Predicates.eval(condition, Seq.empty[Bag])), as)
  }

  def execute(selection: Selection, as: String): Bag = execute(null, selection, as)
}