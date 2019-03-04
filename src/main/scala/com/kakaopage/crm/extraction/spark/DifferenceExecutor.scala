package com.kakaopage.crm.extraction.spark

import com.kakaopage.crm.extraction.ra.Difference

object DifferenceExecutor extends BinaryRelationalAlgebraOperatorExecutor[Difference] {
  override def execute(ds1: RelationDataset, ds2: RelationDataset, difference: Difference, as: String): RelationDataset = {
    RelationDataset(ds1.df.except(ds2.df), as)
  }
}
