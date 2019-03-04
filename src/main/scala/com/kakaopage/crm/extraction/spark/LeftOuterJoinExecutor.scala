package com.kakaopage.crm.extraction.spark

import com.kakaopage.crm.extraction.ra.LeftOuterJoin

object LeftOuterJoinExecutor extends JoinExecutor[LeftOuterJoin] {
  override def joinType: String = "left_outer"
}
