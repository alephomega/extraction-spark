package com.kakaopage.crm.extraction.spark

import com.kakaopage.crm.extraction.ra.Union

object UnionExecutor extends BinaryRelationalAlgebraOperatorExecutor[Union] {
  override def execute(ds1: Bag, ds2: Bag, union: Union, as: String): Bag = {
    Bag(ds1.df.union(ds2.df), as)
  }
}
