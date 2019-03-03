package com.kakaopage.crm.extraction.spark

import com.kakaopage.crm.extraction.ra.Projection
import org.apache.spark.sql.DataFrame

import scala.collection.JavaConverters._

object ProjectionExecutor extends UnaryRelationalAlgebraOperatorExecutor[Projection] {
  override def execute(df: DataFrame, projection: Projection): DataFrame = {
    val attributes = projection.getAttributes.asScala.map(a => Functions.column(a))

    df.select(attributes: _*)
  }
}
