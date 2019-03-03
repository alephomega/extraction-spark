package com.kakaopage.crm.extraction.spark

import com.kakaopage.crm.extraction.ra.Grouping
import org.apache.spark.sql.{Column, DataFrame}

import scala.collection.JavaConverters._

object GroupingExecutor extends UnaryRelationalAlgebraOperatorExecutor[Grouping] {
  override def execute(df: DataFrame, grouping: Grouping): DataFrame = {
    val r = df.groupBy(grouping.getGroupBy.asScala.map(g => Functions.column(g.getBy).as(g.getAlias)): _*)
    val as: Seq[Column] = grouping.getAggregations.asScala.map(a => Functions.column(a.getFunction).as(a.getAlias))

    r.agg(as.head, as.tail: _*)
  }
}
