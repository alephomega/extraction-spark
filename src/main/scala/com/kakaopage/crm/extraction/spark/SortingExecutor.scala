package com.kakaopage.crm.extraction.spark

import com.kakaopage.crm.extraction.ra.Sorting
import org.apache.spark.sql.functions._

import scala.collection.JavaConverters._

object SortingExecutor extends UnaryRelationalAlgebraOperatorExecutor[Sorting] {

  override def execute(ds: Bag, sorting: Sorting, as: String): Bag = {
    val cols = sorting.getOrderings.asScala.map(o => {
      o.getOrderBy.name().toLowerCase match {
        case "asc" => asc(o.getColumn)
        case "desc" => desc(o.getColumn)
      }
    })

    Bag(ds.df.orderBy(cols: _*), as)
  }
}
