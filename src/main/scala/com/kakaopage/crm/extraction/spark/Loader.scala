package com.kakaopage.crm.extraction.spark

import com.amazonaws.services.glue.GlueContext
import com.kakaopage.crm.extraction.ra.relations.Source
import com.kakaopage.crm.extraction.{Catalog, Predicate}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object Loader {
  def load(glueContext: GlueContext, source: Source): DataFrame = {
    source.isManaged match {
      case true =>
        glueContext.getCatalogSource(
          database = Catalog.database(source),
          tableName = Catalog.table(source),
          pushDownPredicate = pushDown(source)).getDynamicFrame().toDF()

      case false =>
        val path = source.getSplit.getPath
        val schema = StructType(List(StructField("customer", StringType, nullable = true)))

        glueContext.read
          .option("header","false")
          .option("delimiter", ",")
          .schema(schema)
          .csv(s"s3://$path")
    }
  }

  def pushDown(source: Source): String = {
    source.getPushDown match {
      case predicate: Predicate => predicate.toPushDownExpression
      case _ => ""
    }
  }
}