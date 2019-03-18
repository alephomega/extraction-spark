package com.kakaopage.crm.extraction.spark

import scalikejdbc.{NamedDB, _}

object Metadata {

  def source(id: String): Option[Source] = NamedDB('cohorts) readOnly { implicit session =>
    sql"""
        select id, database, table, predicate
          from cohorts
         where id = $id
      """.map(rs => Source(rs)).single.apply()
  }
}
