package com.kakaopage.crm.extraction.spark

import org.apache.spark.sql.DataFrame
import scalikejdbc._

case class Source(id: String, database: String, table: String, predicate: String, count: Int) {

  def load(): DataFrame = {
    null
  }
}

object Source extends SQLSyntaxSupport[Source] {
  override val tableName = "cohorts"
  def apply(rs: WrappedResultSet): Source =
    Source(rs.string("id"), rs.string("database"), rs.string("table"), rs.string("predicate"), rs.int("count"))
}