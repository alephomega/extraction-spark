package com.kakaopage.crm.extraction.spark

import com.kakaopage.crm.extraction.ra.LeftOuterJoin
import org.apache.spark.sql.DataFrame

object LeftOuterJoinExecutor extends BinaryRelationalAlgebraOperatorExecutor[LeftOuterJoin] {
  override def execute(df1: DataFrame, df2: DataFrame, leftOuterJoin: LeftOuterJoin): DataFrame = {
    df1.join(df2, Predicates.eval(leftOuterJoin.getCondition), joinType = "left_outer")
  }
}
