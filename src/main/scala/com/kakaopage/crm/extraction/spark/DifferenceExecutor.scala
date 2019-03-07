package com.kakaopage.crm.extraction.spark

import com.kakaopage.crm.extraction.ra.Difference

object DifferenceExecutor extends BinaryRelationalAlgebraOperatorExecutor[Difference] {
  override def execute(ds1: Bag, ds2: Bag, difference: Difference, as: String): Bag = {
    Bag(ds1.df.except(ds2.df), as)
  }
}
