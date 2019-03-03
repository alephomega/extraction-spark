package com.kakaopage.crm.extraction.spark

import com.kakaopage.crm.extraction.ra.Difference
import org.apache.spark.sql.DataFrame

object DifferenceExecutor extends BinaryRelationalAlgebraOperatorExecutor[Difference] {
  override def execute(df1: DataFrame, df2: DataFrame, difference: Difference): DataFrame = {
    df1.except(df2)
  }
}
