package com.kakaopage.crm.extraction.spark

import com.kakaopage.crm.extraction.ra.SemiJoin

object SemiJoinExecutor extends JoinExecutor[SemiJoin] {
  override def joinType: String = "left_semi"
}
