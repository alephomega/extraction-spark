package com.kakaopage.crm.extraction.spark

import com.kakaopage.crm.extraction.ra.Join
import org.apache.spark.sql._

trait JoinExecutor[T <: Join] extends BinaryRelationalAlgebraOperatorExecutor[T] {
  def joinType: String

  def deleteCol(j: DataFrame, r: DataFrame, cols: Seq[String]): DataFrame = {
    if (cols.size == 0) j else deleteCol(j.drop(r(cols.head)), r, cols.tail)
  }

  override def execute(ds1: Bag, ds2: Bag, join: T, as: String): Bag = {
    val df = ds1.df.join(ds2.df, Predicates.eval(join.getCondition, Seq(ds1, ds2)), joinType = joinType)

    val cols = df.columns.groupBy(identity).mapValues(_.size).filter(_._2 > 1).keySet.toSeq
    Bag(deleteCol(df, ds2.df, cols), as)
  }
}
