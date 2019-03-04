package com.kakaopage.crm.extraction.spark

import com.kakaopage.crm.extraction.ra.DuplicateElimination

object DuplicateEliminationExecutor extends UnaryRelationalAlgebraOperatorExecutor[DuplicateElimination] {
  override def execute(ds: RelationDataset, operator: DuplicateElimination, as: String): RelationDataset = {
    RelationDataset(ds.df.distinct(), as)
  }
}
