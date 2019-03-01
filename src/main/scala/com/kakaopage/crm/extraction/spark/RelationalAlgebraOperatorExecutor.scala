package com.kakaopage.crm.extraction.spark

import com.kakaopage.crm.extraction.ra.RelationalAlgebraOperator
import org.apache.spark.sql.DataFrame

abstract class RelationalAlgebraOperatorExecutor[T <: RelationalAlgebraOperator] {

  def execute(df: DataFrame, operator: T): DataFrame
}
