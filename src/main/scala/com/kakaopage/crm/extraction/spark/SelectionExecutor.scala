package com.kakaopage.crm.extraction.spark

import com.kakaopage.crm.extraction.ra.Selection
import org.apache.spark.sql.DataFrame

class SelectionExecutor extends RelationalAlgebraOperatorExecutor[Selection] {

  override def execute(df: DataFrame, selection: Selection): DataFrame = {
    val condition = selection.getCondition

    null
  }
}
