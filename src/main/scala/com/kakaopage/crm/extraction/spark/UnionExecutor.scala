package com.kakaopage.crm.extraction.spark

import com.kakaopage.crm.extraction.ra.Union

object UnionExecutor extends BinaryRelationalAlgebraOperatorExecutor[Union] {
  override def execute(ds1: RelationDataset, ds2: RelationDataset, union: Union, as: String): RelationDataset = {
    RelationDataset(ds1.df.union(ds2.df), as)
  }
}
