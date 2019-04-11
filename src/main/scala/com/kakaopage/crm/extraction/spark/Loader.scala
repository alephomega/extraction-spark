package com.kakaopage.crm.extraction.spark

import com.amazonaws.services.glue.GlueContext
import com.kakaopage.crm.extraction.ra.relations.Source
import com.kakaopage.crm.extraction.{Catalog, Predicate}
import org.apache.spark.sql.DataFrame

object Loader {
  def load(glueContext: GlueContext, source: Source): DataFrame = {
    glueContext.getCatalogSource(
      database = Catalog.database(source),
      tableName = Catalog.table(source),
      pushDownPredicate = pushDown(source)).getDynamicFrame().toDF()
  }

  def pushDown(source: Source): String = {
    source.getPushDown match {
      case predicate: Predicate => predicate.toPushDownExpression
      case _ => ""
    }
  }
}
