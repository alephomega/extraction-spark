package com.kakaopage.crm.extraction.spark

import com.amazonaws.services.glue.GlueContext
import com.kakaopage.crm.extraction.Catalog
import com.kakaopage.crm.extraction.ra.Source
import org.apache.spark.sql.DataFrame

object Loader {

  def load(glueContext: GlueContext, source: Source): DataFrame = {
    glueContext.getCatalogSource(
      database = Catalog.database(source),
      tableName = Catalog.table(source),
      pushDownPredicate = Catalog.pushDown(source)).getDynamicFrame().toDF()
  }
}
