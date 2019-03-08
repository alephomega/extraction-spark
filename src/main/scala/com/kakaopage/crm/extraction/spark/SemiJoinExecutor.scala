package com.kakaopage.crm.extraction.spark

import com.kakaopage.crm.extraction.ra.SemiJoin

class SemiJoinExecutor extends JoinExecutor[SemiJoin] {
  override def joinType: String = "left_semi"
}
