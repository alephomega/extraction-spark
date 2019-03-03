package com.kakaopage.crm.extraction.spark

import com.kakaopage.crm.extraction.ra.Renaming
import org.apache.spark.sql.DataFrame

import scala.collection.JavaConverters._

object RenamingExecutor extends UnaryRelationalAlgebraOperatorExecutor[Renaming] {
  override def execute(df: DataFrame, renaming: Renaming): DataFrame = {
    renaming.getChanges.asScala.foldLeft(df)((r, c) => r.withColumnRenamed(Renaming.from(c), Renaming.to(c)))
  }
}
