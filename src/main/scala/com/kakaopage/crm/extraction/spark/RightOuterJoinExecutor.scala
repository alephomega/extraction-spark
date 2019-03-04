package com.kakaopage.crm.extraction.spark

import com.kakaopage.crm.extraction.ra.RightOuterJoin

object RightOuterJoinExecutor extends JoinExecutor[RightOuterJoin] {
  override def joinType: String = "right_outer"
}