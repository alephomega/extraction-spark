package com.kakaopage.crm.extraction.spark

import com.kakaopage.crm.extraction.ra.Relation

object Metadata {

  def source(rel: Relation): Option[Source] = {
    val predicate = rel.getPushDown
    val pushDownExpression = if (predicate == null) "" else predicate.toPushDownExpression
    val id: String = ???
    val database: String = ???
    val table: String = ???
    val count: Long = ???

    Some(Source(id, database, table, pushDownExpression, count))
  }
}
