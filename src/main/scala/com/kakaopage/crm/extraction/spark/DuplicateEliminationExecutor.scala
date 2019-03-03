package com.kakaopage.crm.extraction.spark

import com.kakaopage.crm.extraction.ra.DuplicateElimination
import org.apache.spark.sql.DataFrame

object DuplicateEliminationExecutor extends UnaryRelationalAlgebraOperatorExecutor[DuplicateElimination] {
  override def execute(df: DataFrame, operator: DuplicateElimination): DataFrame = {
    df.distinct()
  }
}
