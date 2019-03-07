package com.kakaopage.crm.extraction.spark

import com.kakaopage.crm.extraction.ra.Grouping
import org.apache.spark.sql.Column

import scala.collection.JavaConverters._

object GroupingExecutor extends UnaryRelationalAlgebraOperatorExecutor[Grouping] {
  override def execute(ds: Bag, grouping: Grouping, as: String): Bag = {

    val r = ds.df.groupBy(grouping.getGroupBy.asScala.map(g =>
      Functions.column(g.getBy, Seq(ds)).as(g.getAlias)): _*)

    val a: Seq[Column] = grouping.getAggregations.asScala.map(a =>
      Functions.column(a.getFunction, Seq(ds)).as(a.getAlias))

    Bag(r.agg(a.head, a.tail: _*), as)
  }
}
