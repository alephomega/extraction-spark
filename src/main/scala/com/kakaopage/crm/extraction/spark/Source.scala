package com.kakaopage.crm.extraction.spark

import com.amazonaws.services.glue.{DataSource, GlueContext}
import org.apache.spark.sql.DataFrame

case class Source(id: String, database: String, table: String, pushDown: String, count: Long) {

  def load(glueContext: GlueContext): DataFrame = {
    source(glueContext).getDynamicFrame().toDF()
  }

  private def source(glueContext: GlueContext): DataSource = {
    glueContext.getCatalogSource(
      database = database,
      tableName = table,
      pushDownPredicate = pushDown)
  }
}