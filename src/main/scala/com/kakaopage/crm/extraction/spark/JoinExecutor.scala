package com.kakaopage.crm.extraction.spark

import com.kakaopage.crm.extraction.ra.Join
import org.apache.spark.sql._

trait JoinExecutor[T <: Join] extends BinaryRelationalAlgebraOperatorExecutor[T] {
  def joinType: String

  def deleteCol(j: DataFrame, r: DataFrame, cols: Seq[String]): DataFrame = {
    if (cols.isEmpty) j else deleteCol(j.drop(r(cols.head)), r, cols.tail)
  }

  def duplicatesAllowed = true
  def removeDuplicates(j: DataFrame, f: DataFrame, s: DataFrame): DataFrame = {
    deleteCol(j, s, j.columns.groupBy(identity).mapValues(_.length).filter(_._2 > 1).keySet.toSeq)
  }

  override def execute(ds1: Bag, ds2: Bag, join: T, as: String): Bag = {
    val df = ds1.df.join(ds2.df, Predicates.eval(join.getCondition, Seq(ds1, ds2)), joinType = joinType)
    Bag(if (join.isDuplicatesAllowed) df else removeDuplicates(df, ds1.df, ds2.df), as)
  }
}
