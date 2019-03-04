package com.kakaopage.crm.extraction.spark

import com.kakaopage.crm.extraction.ra.{Relation, Selection}
import org.apache.spark.sql.DataFrame

object SelectionExecutor extends UnaryRelationalAlgebraOperatorExecutor[Selection] {
  def source(rel: Relation): DataFrame = ???

  override def execute(nv: RelationDataset, selection: Selection, as: String): RelationDataset = {
    val rel = selection.getRelation
    val condition = selection.getCondition

    RelationDataset(source(rel).filter(Predicates.eval(condition, Seq.empty[RelationDataset])), as)
  }
}
