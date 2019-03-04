package com.kakaopage.crm.extraction.spark

import com.kakaopage.crm.extraction.ra.FullOuterJoin

object FullOuterJoinExecutor extends JoinExecutor[FullOuterJoin] {
  override def joinType: String = "outer"
}
