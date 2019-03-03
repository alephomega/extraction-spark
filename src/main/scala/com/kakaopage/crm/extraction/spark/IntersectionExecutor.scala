package com.kakaopage.crm.extraction.spark

import com.kakaopage.crm.extraction.ra.Intersection
import org.apache.spark.sql.DataFrame

object IntersectionExecutor extends BinaryRelationalAlgebraOperatorExecutor[Intersection] {
  override def execute(df1: DataFrame, df2: DataFrame, intersection: Intersection): DataFrame = {
    df1.intersect(df2)
  }
}
