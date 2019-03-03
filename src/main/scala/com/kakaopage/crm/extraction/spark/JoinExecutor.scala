package com.kakaopage.crm.extraction.spark

import com.kakaopage.crm.extraction.ra.Join
import org.apache.spark.sql.DataFrame

object JoinExecutor extends BinaryRelationalAlgebraOperatorExecutor[Join] {
  override def execute(df1: DataFrame, df2: DataFrame, join: Join): DataFrame = {
    df1.join(df2, Predicates.eval(join.getCondition))
  }
}