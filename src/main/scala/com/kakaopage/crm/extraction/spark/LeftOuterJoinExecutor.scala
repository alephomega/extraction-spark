package com.kakaopage.crm.extraction.spark

import com.kakaopage.crm.extraction.ra.LeftOuterJoin
import org.apache.spark.sql.DataFrame

object LeftOuterJoinExecutor extends JoinExecutor[LeftOuterJoin] {
  override def joinType: String = "left_outer"
}
