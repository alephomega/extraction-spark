package com.kakaopage.crm.extraction.spark

import com.kakaopage.crm.extraction.ra.Product
import org.apache.spark.sql.DataFrame

object ProductExecutor extends BinaryRelationalAlgebraOperatorExecutor[Product] {
  override def execute(df1: DataFrame, df2: DataFrame, product: Product): DataFrame = {
    df1.join(df2)
  }
}
