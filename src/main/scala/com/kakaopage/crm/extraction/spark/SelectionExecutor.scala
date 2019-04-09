package com.kakaopage.crm.extraction.spark

import com.kakaopage.crm.extraction.predicates.True
import com.kakaopage.crm.extraction.ra.Selection

object SelectionExecutor extends UnaryRelationalAlgebraOperatorExecutor[Selection] {

  override def execute(ds: Bag, selection: Selection, as: String): Bag = {
    val condition = selection.getCondition
    Bag(ds.df.filter(Predicates.eval(if (condition == null) new True() else condition, Seq(ds))), as)
  }
}