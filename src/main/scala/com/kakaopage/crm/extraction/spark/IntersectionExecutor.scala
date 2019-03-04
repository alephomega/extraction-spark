package com.kakaopage.crm.extraction.spark

import com.kakaopage.crm.extraction.ra.Intersection

object IntersectionExecutor extends BinaryRelationalAlgebraOperatorExecutor[Intersection] {
  override def execute(ds1: RelationDataset, ds2: RelationDataset, intersection: Intersection, as: String): RelationDataset = {
    RelationDataset(ds1.df.intersect(ds2.df), as)
  }
}
