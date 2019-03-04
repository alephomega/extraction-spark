package com.kakaopage.crm.extraction.spark

import com.kakaopage.crm.extraction.ra.Product
import org.apache.spark.sql.DataFrame

object ProductExecutor extends BinaryRelationalAlgebraOperatorExecutor[Product] {
  override def execute(ds1: RelationDataset, ds2: RelationDataset, product: Product, as: String): RelationDataset = {
    RelationDataset(ds1.df.join(ds2.df), as)
  }
}
