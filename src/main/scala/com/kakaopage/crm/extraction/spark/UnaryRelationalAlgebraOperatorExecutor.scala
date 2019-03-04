package com.kakaopage.crm.extraction.spark

import com.kakaopage.crm.extraction.ra.{Relation, RelationalAlgebraOperator}
import org.apache.spark.sql._

abstract class UnaryRelationalAlgebraOperatorExecutor[T <: RelationalAlgebraOperator] extends RelationalAlgebraOperatorExecutor {
  def execute(ds: RelationDataset, operator: T, as: String): RelationDataset
}
