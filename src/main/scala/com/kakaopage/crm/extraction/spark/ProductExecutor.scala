package com.kakaopage.crm.extraction.spark

import com.kakaopage.crm.extraction.ra.Product

object ProductExecutor extends BinaryRelationalAlgebraOperatorExecutor[Product] {
  override def execute(ds1: Bag, ds2: Bag, product: Product, as: String): Bag = {
    Bag(ds1.df.crossJoin(ds2.df), as)
  }
}
