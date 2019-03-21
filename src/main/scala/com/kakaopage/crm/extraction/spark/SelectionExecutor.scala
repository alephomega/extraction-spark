package com.kakaopage.crm.extraction.spark

import com.amazonaws.services.glue.GlueContext
import com.kakaopage.crm.extraction.ra.Selection

object SelectionExecutor extends UnaryRelationalAlgebraOperatorExecutor[Selection] {

  override def execute(ds: Bag, selection: Selection, as: String): Bag = {
    val condition = selection.getCondition
    Bag(ds.df.filter(Predicates.eval(condition, Seq(ds))), as)
  }

  def execute(glueContext: GlueContext, selection: Selection, as: String): Bag = {
    val source = selection.getSource
    execute(Bag(Loader.load(glueContext, source), source.getName), selection, as)
  }
}