package com.kakaopage.crm.extraction.spark

import com.kakaopage.crm.extraction.ra.RightOuterJoin
import org.apache.spark.sql.DataFrame

object RightOuterJoinExecutor extends BinaryRelationalAlgebraOperatorExecutor[RightOuterJoin] {
  override def execute(df1: DataFrame, df2: DataFrame, rightOuterJoin: RightOuterJoin): DataFrame = {
    df1.join(df2, Predicates.eval(rightOuterJoin.getCondition), joinType = "right_outer")
  }
}
