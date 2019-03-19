package com.kakaopage.crm.extraction.spark

import com.kakaopage.crm.extraction.ra.DuplicateElimination

object DuplicateEliminationExecutor extends UnaryRelationalAlgebraOperatorExecutor[DuplicateElimination] {
  override def execute(ds: Bag, operator: DuplicateElimination, as: String): Bag = {
    Bag(ds.df.distinct(), as)
  }
}