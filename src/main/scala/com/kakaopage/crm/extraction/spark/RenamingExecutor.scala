package com.kakaopage.crm.extraction.spark

import com.kakaopage.crm.extraction.ra.Renaming

import scala.collection.JavaConverters._

object RenamingExecutor extends UnaryRelationalAlgebraOperatorExecutor[Renaming] {
  override def execute(ds: Bag, renaming: Renaming, as: String): Bag = {
    Bag(renaming.getChanges.asScala.foldLeft(ds.df)((r, c) => r.withColumnRenamed(Renaming.from(c), Renaming.to(c))), as)
  }
}
