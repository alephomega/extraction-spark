package com.kakaopage.crm.extraction.spark

import com.amazonaws.services.glue.GlueContext
import com.kakaopage.crm.extraction.ra.{Relation, Selection}
import org.apache.spark.sql.DataFrame

object SelectionExecutor extends UnaryRelationalAlgebraOperatorExecutor[Selection] {

  def source(glueContext: GlueContext, rel: Relation): DataFrame = {
    Metadata.source(rel) match {
      case Some(s) => s.load(glueContext)
      case _ => throw new ExtractionException(f"No metadata for relation: ${rel.getName}%s was found")
    }
  }

  override def execute(ds: Bag, selection: Selection, as: String): Bag = {
    val condition = selection.getCondition
    Bag(ds.df.filter(Predicates.eval(condition, Seq(ds))), as)
  }

  def execute(glueContext: GlueContext, selection: Selection, as: String): Bag = {
    val rel: Relation = selection.getRelation
    execute(Bag(source(glueContext, rel), rel.getName), selection, as)
  }
}