package com.kakaopage.crm.extraction.spark

import com.kakaopage.crm.extraction.ra.Intersection

object IntersectionExecutor extends BinaryRelationalAlgebraOperatorExecutor[Intersection] {
  override def execute(ds1: Bag, ds2: Bag, intersection: Intersection, as: String): Bag = {
    Bag(ds1.df.intersect(ds2.df), as)
  }
}
