package com.kakaopage.crm.extraction.spark

import com.kakaopage.crm.extraction.ra.{Relation, RelationalAlgebraOperator}
import org.apache.spark.sql.DataFrame

abstract class BinaryRelationalAlgebraOperatorExecutor[T <: RelationalAlgebraOperator] extends RelationalAlgebraOperatorExecutor {
  def execute(df1: DataFrame, df2: DataFrame, operator: T): DataFrame
}
