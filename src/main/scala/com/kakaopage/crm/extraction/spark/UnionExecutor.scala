package com.kakaopage.crm.extraction.spark

import com.kakaopage.crm.extraction.ra.Union
import org.apache.spark.sql.DataFrame

object UnionExecutor extends BinaryRelationalAlgebraOperatorExecutor[Union] {
  override def execute(df1: DataFrame, df2: DataFrame, union: Union): DataFrame = {
    df1.union(df2)
  }
}
