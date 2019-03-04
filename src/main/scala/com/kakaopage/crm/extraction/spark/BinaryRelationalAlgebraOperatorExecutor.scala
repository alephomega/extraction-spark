package com.kakaopage.crm.extraction.spark

import com.kakaopage.crm.extraction.ra.RelationalAlgebraOperator

abstract class BinaryRelationalAlgebraOperatorExecutor[T <: RelationalAlgebraOperator] extends RelationalAlgebraOperatorExecutor {
  def execute(ds1: RelationDataset, ds2: RelationDataset, operator: T, as: String): RelationDataset
}
