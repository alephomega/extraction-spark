package com.kakaopage.crm.extraction.spark

import com.kakaopage.crm.extraction.ra.FullOuterJoin
import org.apache.spark.sql.DataFrame

object FullOuterJoinExecutor extends BinaryRelationalAlgebraOperatorExecutor[FullOuterJoin] {
  override def execute(df1: DataFrame, df2: DataFrame, fullOuterJoin: FullOuterJoin): DataFrame = {
    df1.join(df2, Predicates.eval(fullOuterJoin.getCondition), joinType = "outer")
  }
}
