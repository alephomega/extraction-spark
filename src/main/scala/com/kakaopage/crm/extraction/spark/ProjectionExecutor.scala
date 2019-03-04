package com.kakaopage.crm.extraction.spark

import com.kakaopage.crm.extraction.functions.Alias
import com.kakaopage.crm.extraction.ra.Projection

import scala.collection.JavaConverters._

object ProjectionExecutor extends UnaryRelationalAlgebraOperatorExecutor[Projection] {

  override def execute(ds: RelationDataset, projection: Projection, as: String): RelationDataset = {
    val attributes = projection.getAttributes.asScala.map(a => {
      val alias = a.asInstanceOf[Alias]
      Functions.column(alias.getFunction, Seq(ds)).as(alias.getName)
    })

    RelationDataset(ds.df.select(attributes: _*), as)
  }
}
